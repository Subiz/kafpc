package kafpc

import (
	"context"
	"git.subiz.net/errors"
	ugrpc "git.subiz.net/goutils/grpc"
	pb "git.subiz.net/header/kafpc"
	"git.subiz.net/idgen"
	"git.subiz.net/kafka"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"hash/crc32"
	"net"
	"strconv"
	"time"
)

type Client struct {
	service  string
	topic    string
	pub      *kafka.Publisher
	sendchan map[uint32]chan Message
	recvchan map[uint32]chan *pb.Response
	donesend map[uint32]chan bool
	host     string
	size     uint32
}

func NewClient(service string, brokers []string, ip, topic string, port int) *Client {
	sendchan := make(map[uint32]chan Message)
	recvchan := make(map[uint32]chan *pb.Response)
	donesend := make(map[uint32]chan bool)
	c := &Client{
		service:  service,
		topic:    topic,
		pub:      kafka.NewPublisher(brokers),
		sendchan: sendchan,
		donesend: donesend,
		recvchan: recvchan,
		host:     ip + ":" + strconv.Itoa(port),
		size:     uint32(10000),
	}

	for i := uint32(0); i < c.size; i++ {
		sendchan[i] = make(chan Message)
		recvchan[i] = make(chan *pb.Response)
		donesend[i] = make(chan bool)
	}
	go c.runSend()
	go c.runRecv()
	return c
}

type Message struct {
	payload proto.Message
	par     int32
	key     string
}

var crc32q = crc32.MakeTable(0xD5828281)
var TimeoutErr = errors.New(500, errors.E_kafka_rpc_timeout)

func (c *Client) Call(path string, param, output proto.Message, key string) error {
	path = c.service + path
	ReqCounter.WithLabelValues(c.service, path).Inc()
	data, err := proto.Marshal(param)
	if err != nil {
		return errors.Wrap(err, 500, errors.E_proto_marshal_error, "input")
	}

	rid := idgen.NewRequestID()
	req := &pb.Request{
		Id:           rid,
		ResponseHost: c.host,
		Body:         data,
		Path:         path,
		Created:      time.Now().UnixNano(),
		Forget:       false,
	}

	mod := crc32.Checksum([]byte(rid), crc32q) % c.size
	c.sendchan[mod] <- Message{req, -1, key}
	<-c.donesend[mod]

	// fire and forget
	if c.host == ":0" {
		return nil
	}

	var outb, errb []byte
	for {
		select {
		case res := <-c.recvchan[mod]:
			if res.GetRequestId() != rid {
				continue
			}

			haserr := "false"
			if res.GetCode() != 0 {
				haserr = "true"
			}
			RepCounter.WithLabelValues(c.service, path, haserr).Inc()
			TotalDuration.WithLabelValues(c.service, path, haserr).
				Observe(float64(time.Since(time.Unix(0, req.GetCreated())) / 1000000))

			outb = res.GetBody()
			errb = res.GetError()
			goto exitfor
		case <-time.After(60 * time.Second):
		}
		TotalDuration.WithLabelValues(c.service, path, "timeout").
			Observe(float64(time.Since(time.Unix(0, req.GetCreated())) / 1000000))
		return TimeoutErr
	}
exitfor:
	if len(errb) != 0 {
		return errors.FromString(string(errb))
	}

	if err = proto.Unmarshal(outb, output); err != nil {
		return errors.Wrap(err, 500, errors.E_proto_marshal_error)
	}
	return nil
}

func (c *Client) runSend() {
	for i := uint32(0); i < c.size; i++ {
		go func(i uint32) {
			for {
				mes := <-c.sendchan[i]
				func() {
					defer func() {
						c.donesend[i] <- true
					}()
					c.pub.Publish(c.topic, mes.payload, mes.par, mes.key)
				}()
			}
		}(i)
	}
}

func (c *Client) runRecv() {
	grpcServer := grpc.NewServer(ugrpc.NewRecoveryInterceptor())
	pb.RegisterKafpcServer(grpcServer, c)

	lis, err := net.Listen("tcp", c.host)
	if err != nil {
		panic(err)
	}

	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}
}

func (c *Client) Reply(_ context.Context, res *pb.Response) (*pb.Empty, error) {
	mod := crc32.Checksum([]byte(res.GetRequestId()), crc32q) % c.size
	c.recvchan[mod] <- res

	return new(pb.Empty), nil
}
