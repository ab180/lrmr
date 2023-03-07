package lrmrpb

import (
	fmt "fmt"

	ghproto "github.com/golang/protobuf/proto" //nolint:staticcheck
	proto "google.golang.org/protobuf/proto"
)

// Name is the name registered for the proto compressor.
const Name = "proto"

type codec struct{}

type vtprotoMessage interface {
	MarshalVT() ([]byte, error)
	UnmarshalVT([]byte) error
}

func (codec) Marshal(v any) ([]byte, error) {
	vt, ok := v.(vtprotoMessage)
	if ok {
		return vt.MarshalVT()
	}

	vv, ok := v.(proto.Message)
	if ok {
		return proto.Marshal(vv)
	}

	vgh, ok := v.(ghproto.Message)
	if ok {
		return ghproto.Marshal(vgh)
	}

	return nil, fmt.Errorf("failed to marshal, message is %T", v)
}

func (codec) Unmarshal(data []byte, v any) error {
	vt, ok := v.(vtprotoMessage)
	if ok {
		return vt.UnmarshalVT(data)
	}

	vv, ok := v.(proto.Message)
	if ok {
		return proto.Unmarshal(data, vv)
	}

	vgh, ok := v.(ghproto.Message)
	if ok {
		return ghproto.Unmarshal(data, vgh)
	}

	return fmt.Errorf("failed to unmarshal, message is %T", v)
}

func (codec) String() string {
	return Name
}

func (codec) Name() string {
	return Name
}
