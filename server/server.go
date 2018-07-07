package server

import (
	"bitbucket.org/subiz/executor"
	pb "bitbucket.org/subiz/header/kafpc"
	"bitbucket.org/subiz/logan/log"
	cmap "bitbucket.org/subiz/map"
	"bitbucket.org/subiz/squasher"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"time"
)

type Job struct {
	*sarama.ConsumerMessage
	req *pb.Request
}

type handlerFunc struct {
	paramType reflect.Type
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
	hs          map[string]handlerFunc
	consumer    Consumer
	exec        *executor.Executor
	sqmap       map[int32]*squasher.Squasher
	clients     cmap.Map
	squashercap uint
}

type R map[fmt.Stringer]interface{}

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

func NewServer(brokers []string, csg string) *Server {
	csm := newHandlerConsumer(brokers, pb.Event_Kafpc_Requested.String(), csg)
	s := &Server{
		RWMutex:     &sync.RWMutex{},
		consumer:    csm,
		squashercap: 10000 * 30 * 2,
		clients:     cmap.New(32),
	}
	s.exec = executor.NewExecutor(10000, 30, s.handleJob)
	return s
}

func (s *Server) handleJob(job executor.Job) {
	mes := job.Data.(Job)
	s.Lock()
	sq := s.createSqIfNotExist(mes.Partition, mes.Offset)
	s.Unlock()
	s.callHandler(s.hs, mes.req)
	sq.Mark(mes.Offset)
}

func convertToHandleFunc(handlers R) map[string]handlerFunc {
	rs := make(map[string]handlerFunc)
	for k, v := range handlers {
		f := reflect.ValueOf(v)
		ptype := f.Type().In(0).Elem()

		pptr := reflect.New(ptype)
		if _, ok := pptr.Interface().(proto.Message); !ok {
			panic("wrong handler for topic " + k.String() +
				". The second param should be type of proto.Message")
		}
		ks := ""
		if k != nil {
			ks = k.String()
		}
		rs[ks] = handlerFunc{paramType: ptype, function: f}
	}
	return rs
}

func (s *Server) Serve(handlers R) error {
	s.hs = convertToHandleFunc(handlers)
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
			j := executor.Job{Key: string(msg.Key), Data: Job{msg, req}}
			s.exec.AddJob(j)
		case <-s.consumer.Notifications():
		case err := <-s.consumer.Errors():
			if err != nil {
				log.Error("kafka error", err)
			}
		case <-EndSignal():
			break loop
		}
	}
	return s.consumer.Close()
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

	body, errb, code := func() (body []byte, errb []byte, code int32) {
		defer func() {
			if r := recover(); r != nil {
				body = []byte(fmt.Sprintf("%v", r))
				code = 5
				errb = body
			}
		}()

		ret := hf.function.Call([]reflect.Value{pptr})
		if len(ret) > 0 {
			body, _ = ret[0].Interface().([]byte)
		}
		if len(ret) > 1 {
			errb, _ = ret[1].Interface().([]byte)
		}
		if len(errb) != 0 {
			code = 1
		}
		return
	}()

	s.callClient(req.GetResponseHost(), &pb.Response{
		RequestId: req.GetId(),
		Body:      body,
		Error:     errb,
		Code:      code,
	})
}

func (s *Server) commitloop(par int32, ofsc <-chan int64) {
	changed, t := false, time.NewTicker(1*time.Second)
	for {
		select {
		case o := <-ofsc:
			changed = true
			m := sarama.ConsumerMessage{
				Topic:     pb.Event_Kafpc_Requested.String(),
				Offset:    o,
				Partition: par,
			}
			s.consumer.MarkOffset(&m, "")
		case <-t.C:
			s.RLock()
			if sq := s.sqmap[par]; sq != nil {
				fmt.Println("Handle status ", par, sq.GetStatus())
			}
			s.RUnlock()
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
	var c pb.KafpcClient
	ci, ok := s.clients.Get(host)
	if !ok {
		c = s.dialClient(host)
		if c == nil {
			return
		}
	} else {
		c = ci.(pb.KafpcClient)
	}
	c.Reply(nil, resp)
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
	opts = append(opts, grpc.WithTimeout(20*time.Second))
	return grpc.Dial(service, opts...)
}
