package lrmrpb

import (
	"context"
	"github.com/ab180/lrmr/lrdd"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"net"
	"strconv"
	"testing"

	"google.golang.org/grpc"
)

func BenchmarkStreamRecv(b *testing.B) {
	benchs := []struct {
		Name string
		Func func(stream Node_PollDataClient) error
	}{
		{
			Name: "recv",
			Func: func(stream Node_PollDataClient) error {
				_, err := stream.Recv()
				if err != nil {
					return err
				}

				return nil
			},
		},
		{
			Name: "recv-pool",
			Func: func(stream Node_PollDataClient) error {
				pollDataResponse := PollDataResponseFromVTPool()
				err := stream.RecvMsg(pollDataResponse)
				if err != nil {
					pollDataResponse.ReturnToVTPool()

					return err
				}

				pollDataResponse.ReturnToVTPool()

				return nil
			},
		},
	}

	for _, bench := range benchs {
		b.Run(bench.Name, func(b *testing.B) {
			clientConn, s, err := newMockNodeClientAndServer()
			if err != nil {
				b.Fatalf("failed to create client and server: %v", err)
			}
			defer clientConn.Close()

			c := NewNodeClient(clientConn)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				stream, err := c.PollData(context.Background())
				if err != nil {
					b.Fatalf("failed to call: %v", err)
				}

				for {
					err := bench.Func(stream)
					if err == io.EOF {
						break
					} else if err != nil {
						b.Fatalf("failed to recv: %v", err)
					}
				}
			}
			b.StopTimer()

			s.Stop()
		})
	}
}

func newMockNodeClientAndServer() (clientConn *grpc.ClientConn, server *grpc.Server, err error) {
	lis, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		return nil, nil, err
	}
	s := grpc.NewServer()

	pollDataResponseMock := &PollDataResponse{}
	for i := 0; i < 1000; i++ {
		pollDataResponseMock.Data = append(pollDataResponseMock.Data, &lrdd.Row{Key: strconv.Itoa(i)})
	}

	RegisterNodeServer(s, &mockNodeServer{
		pollData: func(stream Node_PollDataServer) error {
			for i := 0; i < 100; i++ {
				err := stream.Send(pollDataResponseMock)
				if err != nil {
					panic(err)
				}
			}

			return nil
		},
	})
	go func() {
		if err := s.Serve(lis); err != nil {
			panic(err)
		}
	}()

	clientConn, err = grpc.Dial(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}

	return clientConn, s, nil
}

type mockNodeServer struct {
	pollData func(Node_PollDataServer) error

	UnimplementedNodeServer
}

func (m mockNodeServer) PollData(stream Node_PollDataServer) error {
	return m.pollData(stream)
}
