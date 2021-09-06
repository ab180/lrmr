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
	  protoc -I/usr/local/include --proto_path=$(go list -f '{{ .Dir }}' -m github.com/gogo/protobuf) -I. \
	  		-I$(GOGOPROTO) \
			--gofast_out=Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/duration.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/struct.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types,plugins=grpc,paths=source_relative:. \
			$$PROTO; \
	done

mocks: deps
	@mockery -all -dir pkg/ -output test/mocks -keeptree

test: test-all

test-all:
	@$(GOTEST) -v -count 1 `go list ./... | grep -v test/e2e`

test-e2e:
	@$(GOTEST) -v -count 1 `go list ./test/e2e` $(FLAGS)
