.PHONY: deps fbs proto mocks test test-all test-e2e

FLATBUFFER_SRCS := $(shell find . -name *.fbs)
PROTO_SRCS := $(shell find . -name *.proto)

GOGOPROTO := $(shell go list -m -f "{{.Dir}}" github.com/gogo/protobuf)

# test runner (can be overriden by CI)
GOTEST ?= go test

deps:
ifeq ($(shell which protoc-gen-gofast), )
	@echo "Installing Dependency: protoc-gen-gofast"
	@go install github.com/gogo/protobuf/{proto,protoc-gen-gofast,gogoproto}
endif

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

vet:
	go vet ./...

sec:
	gosec ./...

pre-push: test vet sec

