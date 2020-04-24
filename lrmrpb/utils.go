package lrmrpb

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func DataHeaderFromMetadata(stream grpc.ServerStream) (*DataHeader, error) {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return nil, errors.New("no metadata")
	}
	entries := md.Get("dataHeader")
	if len(entries) < 1 {
		return nil, errors.New("error parsing metadata: dataHeader is required")
	}
	header := new(DataHeader)
	if err := jsoniter.UnmarshalFromString(entries[0], header); err != nil {
		return nil, errors.Wrap(err, "parse dataHeader")
	}
	return header, nil
}
