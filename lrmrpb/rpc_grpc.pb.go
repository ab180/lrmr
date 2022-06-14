// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.21.1
// source: lrmrpb/rpc.proto

package lrmrpb

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// NodeClient is the client API for Node service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type NodeClient interface {
	CreateJob(ctx context.Context, in *CreateJobRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	StartJobInBackground(ctx context.Context, in *StartJobRequest, opts ...grpc.CallOption) (*emptypb.Empty, error)
	StartJobInForeground(ctx context.Context, in *StartJobRequest, opts ...grpc.CallOption) (Node_StartJobInForegroundClient, error)
	PushData(ctx context.Context, opts ...grpc.CallOption) (Node_PushDataClient, error)
	PollData(ctx context.Context, opts ...grpc.CallOption) (Node_PollDataClient, error)
}

type nodeClient struct {
	cc grpc.ClientConnInterface
}

func NewNodeClient(cc grpc.ClientConnInterface) NodeClient {
	return &nodeClient{cc}
}

func (c *nodeClient) CreateJob(ctx context.Context, in *CreateJobRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/lrmrpb.Node/CreateJob", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeClient) StartJobInBackground(ctx context.Context, in *StartJobRequest, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, "/lrmrpb.Node/StartJobInBackground", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nodeClient) StartJobInForeground(ctx context.Context, in *StartJobRequest, opts ...grpc.CallOption) (Node_StartJobInForegroundClient, error) {
	stream, err := c.cc.NewStream(ctx, &Node_ServiceDesc.Streams[0], "/lrmrpb.Node/StartJobInForeground", opts...)
	if err != nil {
		return nil, err
	}
	x := &nodeStartJobInForegroundClient{stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type Node_StartJobInForegroundClient interface {
	Recv() (*JobOutput, error)
	grpc.ClientStream
}

type nodeStartJobInForegroundClient struct {
	grpc.ClientStream
}

func (x *nodeStartJobInForegroundClient) Recv() (*JobOutput, error) {
	m := new(JobOutput)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *nodeClient) PushData(ctx context.Context, opts ...grpc.CallOption) (Node_PushDataClient, error) {
	stream, err := c.cc.NewStream(ctx, &Node_ServiceDesc.Streams[1], "/lrmrpb.Node/PushData", opts...)
	if err != nil {
		return nil, err
	}
	x := &nodePushDataClient{stream}
	return x, nil
}

type Node_PushDataClient interface {
	Send(*PushDataRequest) error
	CloseAndRecv() (*emptypb.Empty, error)
	grpc.ClientStream
}

type nodePushDataClient struct {
	grpc.ClientStream
}

func (x *nodePushDataClient) Send(m *PushDataRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *nodePushDataClient) CloseAndRecv() (*emptypb.Empty, error) {
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	m := new(emptypb.Empty)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *nodeClient) PollData(ctx context.Context, opts ...grpc.CallOption) (Node_PollDataClient, error) {
	stream, err := c.cc.NewStream(ctx, &Node_ServiceDesc.Streams[2], "/lrmrpb.Node/PollData", opts...)
	if err != nil {
		return nil, err
	}
	x := &nodePollDataClient{stream}
	return x, nil
}

type Node_PollDataClient interface {
	Send(*PollDataRequest) error
	Recv() (*PollDataResponse, error)
	grpc.ClientStream
}

type nodePollDataClient struct {
	grpc.ClientStream
}

func (x *nodePollDataClient) Send(m *PollDataRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *nodePollDataClient) Recv() (*PollDataResponse, error) {
	m := new(PollDataResponse)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// NodeServer is the server API for Node service.
// All implementations must embed UnimplementedNodeServer
// for forward compatibility
type NodeServer interface {
	CreateJob(context.Context, *CreateJobRequest) (*emptypb.Empty, error)
	StartJobInBackground(context.Context, *StartJobRequest) (*emptypb.Empty, error)
	StartJobInForeground(*StartJobRequest, Node_StartJobInForegroundServer) error
	PushData(Node_PushDataServer) error
	PollData(Node_PollDataServer) error
	mustEmbedUnimplementedNodeServer()
}

// UnimplementedNodeServer must be embedded to have forward compatible implementations.
type UnimplementedNodeServer struct {
}

func (UnimplementedNodeServer) CreateJob(context.Context, *CreateJobRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method CreateJob not implemented")
}
func (UnimplementedNodeServer) StartJobInBackground(context.Context, *StartJobRequest) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method StartJobInBackground not implemented")
}
func (UnimplementedNodeServer) StartJobInForeground(*StartJobRequest, Node_StartJobInForegroundServer) error {
	return status.Errorf(codes.Unimplemented, "method StartJobInForeground not implemented")
}
func (UnimplementedNodeServer) PushData(Node_PushDataServer) error {
	return status.Errorf(codes.Unimplemented, "method PushData not implemented")
}
func (UnimplementedNodeServer) PollData(Node_PollDataServer) error {
	return status.Errorf(codes.Unimplemented, "method PollData not implemented")
}
func (UnimplementedNodeServer) mustEmbedUnimplementedNodeServer() {}

// UnsafeNodeServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to NodeServer will
// result in compilation errors.
type UnsafeNodeServer interface {
	mustEmbedUnimplementedNodeServer()
}

func RegisterNodeServer(s grpc.ServiceRegistrar, srv NodeServer) {
	s.RegisterService(&Node_ServiceDesc, srv)
}

func _Node_CreateJob_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(CreateJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).CreateJob(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/lrmrpb.Node/CreateJob",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).CreateJob(ctx, req.(*CreateJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Node_StartJobInBackground_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StartJobRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NodeServer).StartJobInBackground(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/lrmrpb.Node/StartJobInBackground",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NodeServer).StartJobInBackground(ctx, req.(*StartJobRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _Node_StartJobInForeground_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(StartJobRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(NodeServer).StartJobInForeground(m, &nodeStartJobInForegroundServer{stream})
}

type Node_StartJobInForegroundServer interface {
	Send(*JobOutput) error
	grpc.ServerStream
}

type nodeStartJobInForegroundServer struct {
	grpc.ServerStream
}

func (x *nodeStartJobInForegroundServer) Send(m *JobOutput) error {
	return x.ServerStream.SendMsg(m)
}

func _Node_PushData_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(NodeServer).PushData(&nodePushDataServer{stream})
}

type Node_PushDataServer interface {
	SendAndClose(*emptypb.Empty) error
	Recv() (*PushDataRequest, error)
	grpc.ServerStream
}

type nodePushDataServer struct {
	grpc.ServerStream
}

func (x *nodePushDataServer) SendAndClose(m *emptypb.Empty) error {
	return x.ServerStream.SendMsg(m)
}

func (x *nodePushDataServer) Recv() (*PushDataRequest, error) {
	m := new(PushDataRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func _Node_PollData_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(NodeServer).PollData(&nodePollDataServer{stream})
}

type Node_PollDataServer interface {
	Send(*PollDataResponse) error
	Recv() (*PollDataRequest, error)
	grpc.ServerStream
}

type nodePollDataServer struct {
	grpc.ServerStream
}

func (x *nodePollDataServer) Send(m *PollDataResponse) error {
	return x.ServerStream.SendMsg(m)
}

func (x *nodePollDataServer) Recv() (*PollDataRequest, error) {
	m := new(PollDataRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Node_ServiceDesc is the grpc.ServiceDesc for Node service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var Node_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "lrmrpb.Node",
	HandlerType: (*NodeServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "CreateJob",
			Handler:    _Node_CreateJob_Handler,
		},
		{
			MethodName: "StartJobInBackground",
			Handler:    _Node_StartJobInBackground_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "StartJobInForeground",
			Handler:       _Node_StartJobInForeground_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "PushData",
			Handler:       _Node_PushData_Handler,
			ClientStreams: true,
		},
		{
			StreamName:    "PollData",
			Handler:       _Node_PollData_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "lrmrpb/rpc.proto",
}