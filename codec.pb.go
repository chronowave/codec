// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.27.1
// 	protoc        v3.12.4
// source: codec.proto

package codec

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

type FlightServiceAction int32

const (
	FlightServiceAction_CreateFlight FlightServiceAction = 0
	FlightServiceAction_DeleteFlight FlightServiceAction = 1
	FlightServiceAction_UpdateSchema FlightServiceAction = 2
	FlightServiceAction_UpdateFile   FlightServiceAction = 3
	//DirectQuery = 4;
	FlightServiceAction_UploadFile FlightServiceAction = 4
)

// Enum value maps for FlightServiceAction.
var (
	FlightServiceAction_name = map[int32]string{
		0: "CreateFlight",
		1: "DeleteFlight",
		2: "UpdateSchema",
		3: "UpdateFile",
		4: "UploadFile",
	}
	FlightServiceAction_value = map[string]int32{
		"CreateFlight": 0,
		"DeleteFlight": 1,
		"UpdateSchema": 2,
		"UpdateFile":   3,
		"UploadFile":   4,
	}
)

func (x FlightServiceAction) Enum() *FlightServiceAction {
	p := new(FlightServiceAction)
	*p = x
	return p
}

func (x FlightServiceAction) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (FlightServiceAction) Descriptor() protoreflect.EnumDescriptor {
	return file_codec_proto_enumTypes[0].Descriptor()
}

func (FlightServiceAction) Type() protoreflect.EnumType {
	return &file_codec_proto_enumTypes[0]
}

func (x FlightServiceAction) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use FlightServiceAction.Descriptor instead.
func (FlightServiceAction) EnumDescriptor() ([]byte, []int) {
	return file_codec_proto_rawDescGZIP(), []int{0}
}

type FlightSchemaRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Flight string `protobuf:"bytes,1,opt,name=flight,proto3" json:"flight,omitempty"`
	Schema []byte `protobuf:"bytes,2,opt,name=schema,proto3" json:"schema,omitempty"`
}

func (x *FlightSchemaRequest) Reset() {
	*x = FlightSchemaRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_codec_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FlightSchemaRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FlightSchemaRequest) ProtoMessage() {}

func (x *FlightSchemaRequest) ProtoReflect() protoreflect.Message {
	mi := &file_codec_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FlightSchemaRequest.ProtoReflect.Descriptor instead.
func (*FlightSchemaRequest) Descriptor() ([]byte, []int) {
	return file_codec_proto_rawDescGZIP(), []int{0}
}

func (x *FlightSchemaRequest) GetFlight() string {
	if x != nil {
		return x.Flight
	}
	return ""
}

func (x *FlightSchemaRequest) GetSchema() []byte {
	if x != nil {
		return x.Schema
	}
	return nil
}

type PutResult struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Seq   []uint64 `protobuf:"varint,1,rep,packed,name=seq,proto3" json:"seq,omitempty"`
	Error []string `protobuf:"bytes,2,rep,name=error,proto3" json:"error,omitempty"`
}

func (x *PutResult) Reset() {
	*x = PutResult{}
	if protoimpl.UnsafeEnabled {
		mi := &file_codec_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *PutResult) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*PutResult) ProtoMessage() {}

func (x *PutResult) ProtoReflect() protoreflect.Message {
	mi := &file_codec_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use PutResult.ProtoReflect.Descriptor instead.
func (*PutResult) Descriptor() ([]byte, []int) {
	return file_codec_proto_rawDescGZIP(), []int{1}
}

func (x *PutResult) GetSeq() []uint64 {
	if x != nil {
		return x.Seq
	}
	return nil
}

func (x *PutResult) GetError() []string {
	if x != nil {
		return x.Error
	}
	return nil
}

type FileTransferRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Flight  string `protobuf:"bytes,1,opt,name=flight,proto3" json:"flight,omitempty"`
	Id      uint64 `protobuf:"varint,2,opt,name=id,proto3" json:"id,omitempty"`
	Replica uint32 `protobuf:"varint,3,opt,name=replica,proto3" json:"replica,omitempty"`
	Data    []byte `protobuf:"bytes,4,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *FileTransferRequest) Reset() {
	*x = FileTransferRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_codec_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *FileTransferRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*FileTransferRequest) ProtoMessage() {}

func (x *FileTransferRequest) ProtoReflect() protoreflect.Message {
	mi := &file_codec_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use FileTransferRequest.ProtoReflect.Descriptor instead.
func (*FileTransferRequest) Descriptor() ([]byte, []int) {
	return file_codec_proto_rawDescGZIP(), []int{2}
}

func (x *FileTransferRequest) GetFlight() string {
	if x != nil {
		return x.Flight
	}
	return ""
}

func (x *FileTransferRequest) GetId() uint64 {
	if x != nil {
		return x.Id
	}
	return 0
}

func (x *FileTransferRequest) GetReplica() uint32 {
	if x != nil {
		return x.Replica
	}
	return 0
}

func (x *FileTransferRequest) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

var File_codec_proto protoreflect.FileDescriptor

var file_codec_proto_rawDesc = []byte{
	0x0a, 0x0b, 0x63, 0x6f, 0x64, 0x65, 0x63, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x45, 0x0a,
	0x13, 0x46, 0x6c, 0x69, 0x67, 0x68, 0x74, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x66, 0x6c, 0x69, 0x67, 0x68, 0x74, 0x18, 0x01,
	0x20, 0x01, 0x28, 0x09, 0x52, 0x06, 0x66, 0x6c, 0x69, 0x67, 0x68, 0x74, 0x12, 0x16, 0x0a, 0x06,
	0x73, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x73, 0x63,
	0x68, 0x65, 0x6d, 0x61, 0x22, 0x33, 0x0a, 0x09, 0x50, 0x75, 0x74, 0x52, 0x65, 0x73, 0x75, 0x6c,
	0x74, 0x12, 0x10, 0x0a, 0x03, 0x73, 0x65, 0x71, 0x18, 0x01, 0x20, 0x03, 0x28, 0x04, 0x52, 0x03,
	0x73, 0x65, 0x71, 0x12, 0x14, 0x0a, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x02, 0x20, 0x03,
	0x28, 0x09, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x22, 0x6b, 0x0a, 0x13, 0x46, 0x69, 0x6c,
	0x65, 0x54, 0x72, 0x61, 0x6e, 0x73, 0x66, 0x65, 0x72, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
	0x12, 0x16, 0x0a, 0x06, 0x66, 0x6c, 0x69, 0x67, 0x68, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09,
	0x52, 0x06, 0x66, 0x6c, 0x69, 0x67, 0x68, 0x74, 0x12, 0x0e, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x02,
	0x20, 0x01, 0x28, 0x04, 0x52, 0x02, 0x69, 0x64, 0x12, 0x18, 0x0a, 0x07, 0x72, 0x65, 0x70, 0x6c,
	0x69, 0x63, 0x61, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x07, 0x72, 0x65, 0x70, 0x6c, 0x69,
	0x63, 0x61, 0x12, 0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0c,
	0x52, 0x04, 0x64, 0x61, 0x74, 0x61, 0x2a, 0x6b, 0x0a, 0x13, 0x46, 0x6c, 0x69, 0x67, 0x68, 0x74,
	0x53, 0x65, 0x72, 0x76, 0x69, 0x63, 0x65, 0x41, 0x63, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x10, 0x0a,
	0x0c, 0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x46, 0x6c, 0x69, 0x67, 0x68, 0x74, 0x10, 0x00, 0x12,
	0x10, 0x0a, 0x0c, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x46, 0x6c, 0x69, 0x67, 0x68, 0x74, 0x10,
	0x01, 0x12, 0x10, 0x0a, 0x0c, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x53, 0x63, 0x68, 0x65, 0x6d,
	0x61, 0x10, 0x02, 0x12, 0x0e, 0x0a, 0x0a, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65, 0x46, 0x69, 0x6c,
	0x65, 0x10, 0x03, 0x12, 0x0e, 0x0a, 0x0a, 0x55, 0x70, 0x6c, 0x6f, 0x61, 0x64, 0x46, 0x69, 0x6c,
	0x65, 0x10, 0x04, 0x42, 0x1e, 0x0a, 0x13, 0x69, 0x6f, 0x2e, 0x63, 0x68, 0x72, 0x6f, 0x6e, 0x6f,
	0x77, 0x61, 0x76, 0x65, 0x2e, 0x63, 0x6f, 0x64, 0x65, 0x63, 0x5a, 0x07, 0x2f, 0x3b, 0x63, 0x6f,
	0x64, 0x65, 0x63, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_codec_proto_rawDescOnce sync.Once
	file_codec_proto_rawDescData = file_codec_proto_rawDesc
)

func file_codec_proto_rawDescGZIP() []byte {
	file_codec_proto_rawDescOnce.Do(func() {
		file_codec_proto_rawDescData = protoimpl.X.CompressGZIP(file_codec_proto_rawDescData)
	})
	return file_codec_proto_rawDescData
}

var file_codec_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_codec_proto_msgTypes = make([]protoimpl.MessageInfo, 3)
var file_codec_proto_goTypes = []interface{}{
	(FlightServiceAction)(0),    // 0: FlightServiceAction
	(*FlightSchemaRequest)(nil), // 1: FlightSchemaRequest
	(*PutResult)(nil),           // 2: PutResult
	(*FileTransferRequest)(nil), // 3: FileTransferRequest
}
var file_codec_proto_depIdxs = []int32{
	0, // [0:0] is the sub-list for method output_type
	0, // [0:0] is the sub-list for method input_type
	0, // [0:0] is the sub-list for extension type_name
	0, // [0:0] is the sub-list for extension extendee
	0, // [0:0] is the sub-list for field type_name
}

func init() { file_codec_proto_init() }
func file_codec_proto_init() {
	if File_codec_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_codec_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FlightSchemaRequest); i {
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
		file_codec_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*PutResult); i {
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
		file_codec_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*FileTransferRequest); i {
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
			RawDescriptor: file_codec_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   3,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_codec_proto_goTypes,
		DependencyIndexes: file_codec_proto_depIdxs,
		EnumInfos:         file_codec_proto_enumTypes,
		MessageInfos:      file_codec_proto_msgTypes,
	}.Build()
	File_codec_proto = out.File
	file_codec_proto_rawDesc = nil
	file_codec_proto_goTypes = nil
	file_codec_proto_depIdxs = nil
}
