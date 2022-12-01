// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.21.1
// source: lrdd/row.proto

package lrdd

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Row struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Key   string `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	Value []byte `protobuf:"bytes,2,opt,name=value,proto3" json:"value,omitempty"`
}

func (x *Row) Reset() {
	*x = Row{}
	if protoimpl.UnsafeEnabled {
		mi := &file_lrdd_row_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Row) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Row) ProtoMessage() {}

func (x *Row) ProtoReflect() protoreflect.Message {
	mi := &file_lrdd_row_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Row.ProtoReflect.Descriptor instead.
func (*Row) Descriptor() ([]byte, []int) {
	return file_lrdd_row_proto_rawDescGZIP(), []int{0}
}

func (x *Row) GetKey() string {
	if x != nil {
		return x.Key
	}
	return ""
}

func (x *Row) GetValue() []byte {
	if x != nil {
		return x.Value
	}
	return nil
}

var File_lrdd_row_proto protoreflect.FileDescriptor

var file_lrdd_row_proto_rawDesc = []byte{
	0x0a, 0x0e, 0x6c, 0x72, 0x64, 0x64, 0x2f, 0x72, 0x6f, 0x77, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x12, 0x04, 0x6c, 0x72, 0x64, 0x64, 0x22, 0x2d, 0x0a, 0x03, 0x52, 0x6f, 0x77, 0x12, 0x10, 0x0a,
	0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12,
	0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x05,
	0x76, 0x61, 0x6c, 0x75, 0x65, 0x42, 0x1c, 0x5a, 0x1a, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e,
	0x63, 0x6f, 0x6d, 0x2f, 0x61, 0x62, 0x31, 0x38, 0x30, 0x2f, 0x6c, 0x72, 0x6d, 0x72, 0x2f, 0x6c,
	0x72, 0x64, 0x64, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_lrdd_row_proto_rawDescOnce sync.Once
	file_lrdd_row_proto_rawDescData = file_lrdd_row_proto_rawDesc
)

func file_lrdd_row_proto_rawDescGZIP() []byte {
	file_lrdd_row_proto_rawDescOnce.Do(func() {
		file_lrdd_row_proto_rawDescData = protoimpl.X.CompressGZIP(file_lrdd_row_proto_rawDescData)
	})
	return file_lrdd_row_proto_rawDescData
}

var file_lrdd_row_proto_msgTypes = make([]protoimpl.MessageInfo, 1)
var file_lrdd_row_proto_goTypes = []interface{}{
	(*Row)(nil), // 0: lrdd.Row
}
var file_lrdd_row_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_lrdd_row_proto_init() }
func file_lrdd_row_proto_init() {
	if File_lrdd_row_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_lrdd_row_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Row); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_lrdd_row_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   1,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_lrdd_row_proto_goTypes,
		DependencyIndexes: file_lrdd_row_proto_depIdxs,
		MessageInfos:      file_lrdd_row_proto_msgTypes,
	}.Build()
	File_lrdd_row_proto = out.File
	file_lrdd_row_proto_rawDesc = nil
	file_lrdd_row_proto_goTypes = nil
	file_lrdd_row_proto_depIdxs = nil
}
