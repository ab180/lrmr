.PHONY: deps fbs proto mocks test test-all test-e2e

FLATBUFFER_SRCS := $(shell find . -name *.fbs)
PROTO_SRCS := $(shell find . -name *.proto)

deps:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.49.0

fbs:
	@for FBS in $(FLATBUFFER_SRCS); do \
	  flatc --go --grpc $$FBS; \
	done

proto:
	@for PROTO in $(PROTO_SRCS); do \
		protoc \
			--go_out=../../.. \
			--plugin protoc-gen-go="${GOBIN}/protoc-gen-go" \
			--go-grpc_out=../../.. \
			--plugin protoc-gen-go-grpc="${GOBIN}/protoc-gen-go-grpc" \
			--go-vtproto_out=../../.. \
			--plugin protoc-gen-go-vtproto="${GOBIN}/protoc-gen-go-vtproto" \
			--go-vtproto_opt=features=marshal+unmarshal+size+pool \
			--go-vtproto_opt=pool=github.com/ab180/lrmr/lrdd.Row \
			--go-vtproto_opt=pool=github.com/ab180/lrmr/lrmrpb.PollDataResponse \
			$$PROTO; \
	done

mocks: deps
	@mockery -all -dir pkg/ -output test/mocks -keeptree

test:
	go test ./...

lint:
	golangci-lint run --enable=lll ./...

vet:
	go vet ./...

sec:
	gosec ./...

pre-push: test lint vet sec
