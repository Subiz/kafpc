package kafpc

import (
	ugrpc "bitbucket.org/subiz/goutils/grpc"
	pb "bitbucket.org/subiz/header/kafpc"
	"bitbucket.org/subiz/id"
	"bitbucket.org/subiz/kafka"
	"context"
	"errors"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"hash/crc32"
	"net"
	"strconv"
	"time"
)

type Client struct {
	pub      *kafka.Publisher
	sendchan map[uint32]chan Message
	recvchan map[uint32]chan *pb.Response
	host     string
	size     uint32
}

func NewClient(brokers []string, ip string, port int) *Client {
	sendchan := make(map[uint32]chan Message)
	recvchan := make(map[uint32]chan *pb.Response)

	c := &Client{
		pub:      kafka.NewPublisher(brokers),
		sendchan: sendchan,
		recvchan: recvchan,
		host:     ip + ":" + strconv.Itoa(port),
		size:     uint32(10000),
	}

	for i := uint32(0); i < c.size; i++ {
		sendchan[i] = make(chan Message, 0)
		recvchan[i] = make(chan *pb.Response, 0)
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
var TimeoutErr = errors.New("kafpc timeout")

func (c *Client) Call(path string, payload proto.Message, par int32, key string) ([]byte, []byte, error) {
	data, err := proto.Marshal(payload)
	if err != nil {
		return nil, nil, err
	}
	rid := ID.NewRequestID()
	req := &pb.Request{Id: rid, ResponseHost: c.host, Body: data, Path: path, Created: time.Now().UnixNano()}

	mod := crc32.Checksum([]byte(rid), crc32q) % c.size
	c.sendchan[mod] <- Message{req, par, key}
	for {
		select {
		case res := <-c.recvchan[mod]:

			if res.GetRequestId() != rid {
				continue
			}
			if res.GetCode() != 0 {
				return res.GetBody(), res.GetError(), nil
			}
			return res.GetBody(), nil, nil
		case <-time.After(20 * time.Second):
		}
		return nil, nil, TimeoutErr
	}
}

func (c *Client) runSend() {
	for i := uint32(0); i < c.size; i++ {
		go func(i uint32) {
			for {
				mes := <-c.sendchan[i]
				c.pub.Publish(pb.Event_Kafpc_Requested.String(), mes.payload, mes.par, mes.key)
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
