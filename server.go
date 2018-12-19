package kafpc

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	"github.com/subiz/errors"
	"github.com/subiz/executor"
	"github.com/subiz/goutils/clock"
	"github.com/subiz/goutils/log"
	cmap "github.com/subiz/goutils/map"
	pb "github.com/subiz/header/kafpc"
	"github.com/subiz/squasher"
	"google.golang.org/grpc"
)

type Job struct {
	*sarama.ConsumerMessage
	req      *pb.Request
	received int64
}

type handlerFunc struct {
	paramType reflect.Type
	firstArg  reflect.Value
	function  reflect.Value
}

type Consumer interface {
	MarkOffset(msg *sarama.ConsumerMessage, metadata string)
	CommitOffsets() error
	Messages() <-chan *sarama.ConsumerMessage
	Notifications() <-chan *cluster.Notification
	Errors() <-chan error
	Close() error
}

type Server struct {
	*sync.RWMutex
	service     string
	hs          map[string]handlerFunc
	consumer    Consumer
	exec        *executor.Executor
	sqmap       map[int32]*squasher.Squasher
	clients     cmap.Map
	squashercap uint
	topic       string
}

func newHandlerConsumer(brokers []string, topic, csg string) *cluster.Consumer {
	c := cluster.NewConfig()
	c.Consumer.MaxWaitTime = 10000 * time.Millisecond
	//c.Consumer.Offsets.Retention = 0
	c.Consumer.Return.Errors = true
	c.Consumer.Offsets.Initial = sarama.OffsetOldest
	c.Group.Session.Timeout = 20 * time.Second
	c.Group.Return.Notifications = true

	for {
		csm, err := cluster.NewConsumer(brokers, csg, []string{topic}, c)
		if err == nil {
			return csm
		}

		log.Warn(err, "will retry...")
		time.Sleep(3 * time.Second)
	}
}

func Serve(service string, brokers []string, csg, topic string, handler interface{}) {
	csm := newHandlerConsumer(brokers, topic, csg)
	s := &Server{
		topic:       topic,
		service:     service,
		RWMutex:     &sync.RWMutex{},
		consumer:    csm,
		squashercap: 10000 * 30 * 2,
		clients:     cmap.New(32),
		sqmap:       make(map[int32]*squasher.Squasher),
	}
	s.exec = executor.NewExecutor(10000, 30, s.handleJob)
	s.register(handler)
}

func (s *Server) handleJob(job executor.Job) {
	mes := job.Data.(Job)
	s.Lock()
	sq := s.createSqIfNotExist(mes.Partition, mes.Offset)
	s.Unlock()

	LagQueueDuration.WithLabelValues(s.service, mes.req.GetPath()).
		Observe(float64(time.Since(time.Unix(0, mes.received)) / 1000000))

	s.callHandler(s.hs, mes.req)
	sq.Mark(mes.Offset)
}

func convertToHandlerFunc(prefix string, handler interface{}) map[string]handlerFunc {
	rs := make(map[string]handlerFunc)
	handlerValue := reflect.ValueOf(handler)
	h := reflect.TypeOf(handler)
	for i := 0; i < h.NumMethod(); i++ {
		method := h.Method(i)
		if !strings.HasPrefix(method.Name, "Handle") {
			continue
		}
		ptype := method.Type.In(1).Elem()
		pptr := reflect.New(ptype)
		if _, ok := pptr.Interface().(proto.Message); !ok {
			panic("wrong handler for topic " + method.Name +
				". The first parameter should be type of proto.Message, got " +
				ptype.Name())
		}
		rs[prefix+method.Name] = handlerFunc{
			paramType: ptype,
			function:  method.Func,
			firstArg:  handlerValue,
		}
	}
	return rs
}

func (s *Server) register(handler interface{}) {
	s.hs = convertToHandlerFunc(s.service, handler)
	endsignal := EndSignal()
loop:
	for {
		select {
		case msg, more := <-s.consumer.Messages():
			if !more || msg == nil {
				break loop
			}

			req := &pb.Request{}
			err := proto.Unmarshal(msg.Value, req)
			if err != nil {
				log.Error(err)
				continue
			}

			received := time.Now().UnixNano()
			s.exec.Add(string(msg.Key), Job{msg, req, received})
		case <-s.consumer.Notifications():
		case err := <-s.consumer.Errors():
			if err != nil {
				log.Error("kafka error", err)
			}
		case <-endsignal:
			break loop
		}
	}
	s.consumer.Close()
}

func EndSignal() chan os.Signal {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	return signals
}

func (s *Server) callHandler(handler map[string]handlerFunc, req *pb.Request) {
	hf, ok := handler[req.GetPath()]
	if !ok || hf.paramType == nil {
		log.Warn("not found hander", req.GetPath())
		return
	}

	pptr := reflect.New(hf.paramType)
	intef := pptr.Interface().(proto.Message)
	if err := proto.Unmarshal(req.GetBody(), intef); err != nil {
		log.Error(err, req.GetBody())
		return
	}

	if time.Since(time.Unix(clock.ToSec(req.GetCreated()), 0)) > 1*time.Minute {
		return
	}

	var body, errb []byte
	starttime := time.Now()
	func() {
		defer func() {
			if r := recover(); r != nil {
				if re, ok := r.(error); ok {
					errb = []byte(errors.Wrap(re, 500, errors.E_unknown).Error())
				} else {
					errb = []byte(errors.New(500, errors.E_unknown, fmt.Sprintf("%v", r)).Error())
				}
				body = []byte(fmt.Sprintf("%v", r))
			}
		}()

		ret := hf.function.Call([]reflect.Value{hf.firstArg, pptr})
		if len(ret) != 2 {
			errb = []byte(errors.New(500, errors.E_kafka_rpc_handler_definition_error, "should returns 2 values (proto.Message, error)").Error())
		}

		err, _ := ret[1].Interface().(error)
		if !ret[1].IsNil() && err != nil {
			errb = []byte(errors.Wrap(err, 400, errors.E_unknown).Error())
			return
		}

		if ret[0].IsNil() {
			return
		}

		if msg, ok := ret[0].Interface().(proto.Message); !ok {
			errb = []byte(errors.New(500, errors.E_kafka_rpc_handler_definition_error, "the first returned value should implement proto.Message interface").Error())
		} else {
			if body, err = proto.Marshal(msg); err != nil {
				errb = []byte(errors.Wrap(err, 500, errors.E_proto_marshal_error).Error())
			}
		}
	}()

	success := "true"
	if len(errb) > 0 {
		success = "false"
	}
	ProcessDuration.WithLabelValues(s.service, req.GetPath(), success).
		Observe(float64(time.Since(starttime) / 1000000))
	if req.Forget {
		return
	}

	if time.Since(time.Unix(clock.ToSec(req.GetCreated()), 0)) > 2*time.Minute {
		return
	}

	s.callClient(req.GetResponseHost(), &pb.Response{
		RequestId: req.GetId(),
		Body:      body,
		Error:     errb,
		Path:      req.GetPath(),
		Created:   time.Now().UnixNano(),
	})
}

func (s *Server) commitloop(par int32, ofsc <-chan int64) {
	changed, t := false, time.NewTicker(1*time.Second)
	for {
		select {
		case o := <-ofsc:
			changed = true
			m := sarama.ConsumerMessage{
				Topic:     s.topic,
				Offset:    o,
				Partition: par,
			}
			s.consumer.MarkOffset(&m, "")
		case <-t.C:
			if changed {
				s.consumer.CommitOffsets()
				changed = false
			}
		}
	}
}

func (s *Server) createSqIfNotExist(par int32, offset int64) *squasher.Squasher {
	if sq := s.sqmap[par]; sq != nil {
		return sq
	}

	sq := squasher.NewSquasher(offset, int32(s.squashercap)) // 1M
	s.sqmap[par] = sq
	go s.commitloop(par, sq.Next())
	return sq
}

func (s *Server) callClient(host string, resp *pb.Response) {
	// call and forget, caller dont want to wait for response
	if host == ":0" {
		return
	}

	var c pb.KafpcClient
	ci, ok := s.clients.Get(host)
	if !ok {
		c = s.dialClient(host)
		if c == nil {
			return
		}
		s.clients.Set(host, c)
	} else {
		c = ci.(pb.KafpcClient)
	}

	clientDeadline := time.Now().Add(1 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)

	go func() {
		defer cancel()
		c.Reply(ctx, resp)
	}()
}

func (s *Server) dialClient(host string) pb.KafpcClient {
	conn, err := dialGrpc(host)
	if err != nil {
		log.Error("unable to connect to "+host+" service", err)
		return nil
	}
	c := pb.NewKafpcClient(conn)
	s.clients.Set(host, c)
	return c
}

func dialGrpc(service string) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	// Enabling WithBlock tells the client to not give up trying to find a server
	opts = append(opts, grpc.WithBlock())
	// However, we're still setting a timeout so that if the server takes too long, we still give up
	opts = append(opts, grpc.WithTimeout(2*time.Second))
	return grpc.Dial(service, opts...)
}
