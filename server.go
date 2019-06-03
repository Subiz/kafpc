package kafpc

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/subiz/errors"
	"github.com/subiz/goutils/clock"
	"github.com/subiz/goutils/log"
	cmap "github.com/subiz/goutils/map"
	"github.com/subiz/header"
	cpb "github.com/subiz/header/common"
	pb "github.com/subiz/header/kafpc"
	"github.com/subiz/kafka"
	"google.golang.org/grpc"
)

type handlerFunc struct {
	paramType reflect.Type
	firstArg  reflect.Value
	function  reflect.Value
}

type Server struct {
	service string
	hs      map[string]handlerFunc
	clients cmap.Map
	topic   string
}

func Serve(service string, brokers []string, csg, topic string, handler interface{}) {
	s := &Server{topic: topic, service: service, clients: cmap.New(32)}

	h := kafka.NewHandler(brokers, csg, topic, true)
	hs := s.convertToHandlerFunc(s.service, handler)

	h.Serve(kafka.H{
		"": func(ctx *cpb.Context, val []byte) {
			req := &pb.Request{}
			if err := proto.Unmarshal(val, req); err != nil {
				return
			}
			s.callHandler(hs[req.GetPath()], req)
		},
	}, func(_ []int32) {})
}

func (s *Server) convertToHandlerFunc(prefix string, handler interface{}) map[string]handlerFunc {
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

func (s *Server) callHandler(hf handlerFunc, req *pb.Request) {
	pptr := reflect.New(hf.paramType)
	intef := pptr.Interface().(proto.Message)
	if err := proto.Unmarshal(req.GetBody(), intef); err != nil {
		log.Error(err, req.GetBody())
		return
	}

	if time.Since(time.Unix(clock.UnixSec(req.GetCreated()), 0)) > 1*time.Minute {
		return
	}

	var body, errb []byte
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

	if req.Forget {
		return
	}

	if time.Since(time.Unix(clock.UnixSec(req.GetCreated()), 0)) > 2*time.Minute {
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

func (s *Server) callClient(host string, resp *pb.Response) {
	// call and forget, caller dont want to wait for response
	if host == ":0" {
		return
	}

	var c header.KafpcClient
	ci, ok := s.clients.Get(host)
	if !ok {
		c = s.dialClient(host)
		if c == nil {
			return
		}
		s.clients.Set(host, c)
	} else {
		c = ci.(header.KafpcClient)
	}

	clientDeadline := time.Now().Add(1 * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), clientDeadline)

	go func() {
		defer cancel()
		c.Reply(ctx, resp)
	}()
}

func (s *Server) dialClient(host string) header.KafpcClient {
	conn, err := dialGrpc(host)
	if err != nil {
		log.Error("unable to connect to "+host+" service", err)
		return nil
	}
	c := header.NewKafpcClient(conn)
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
