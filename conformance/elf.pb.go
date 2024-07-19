// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.34.2
// 	protoc        v3.21.12
// source: elf.proto

package conformance

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

type ELFBinary struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Data []byte `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
}

func (x *ELFBinary) Reset() {
	*x = ELFBinary{}
	if protoimpl.UnsafeEnabled {
		mi := &file_elf_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ELFBinary) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ELFBinary) ProtoMessage() {}

func (x *ELFBinary) ProtoReflect() protoreflect.Message {
	mi := &file_elf_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ELFBinary.ProtoReflect.Descriptor instead.
func (*ELFBinary) Descriptor() ([]byte, []int) {
	return file_elf_proto_rawDescGZIP(), []int{0}
}

func (x *ELFBinary) GetData() []byte {
	if x != nil {
		return x.Data
	}
	return nil
}

// Wrapper for the ELF binary and the features that the loader should use
// Note that we currently hardcode the features to be used by the loader,
// so features isn't actually used yet.
type ELFLoaderCtx struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Elf          *ELFBinary  `protobuf:"bytes,1,opt,name=elf,proto3" json:"elf,omitempty"`
	Features     *FeatureSet `protobuf:"bytes,2,opt,name=features,proto3" json:"features,omitempty"`
	ElfSz        uint64      `protobuf:"varint,3,opt,name=elf_sz,json=elfSz,proto3" json:"elf_sz,omitempty"`
	DeployChecks bool        `protobuf:"varint,4,opt,name=deploy_checks,json=deployChecks,proto3" json:"deploy_checks,omitempty"`
}

func (x *ELFLoaderCtx) Reset() {
	*x = ELFLoaderCtx{}
	if protoimpl.UnsafeEnabled {
		mi := &file_elf_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ELFLoaderCtx) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ELFLoaderCtx) ProtoMessage() {}

func (x *ELFLoaderCtx) ProtoReflect() protoreflect.Message {
	mi := &file_elf_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ELFLoaderCtx.ProtoReflect.Descriptor instead.
func (*ELFLoaderCtx) Descriptor() ([]byte, []int) {
	return file_elf_proto_rawDescGZIP(), []int{1}
}

func (x *ELFLoaderCtx) GetElf() *ELFBinary {
	if x != nil {
		return x.Elf
	}
	return nil
}

func (x *ELFLoaderCtx) GetFeatures() *FeatureSet {
	if x != nil {
		return x.Features
	}
	return nil
}

func (x *ELFLoaderCtx) GetElfSz() uint64 {
	if x != nil {
		return x.ElfSz
	}
	return 0
}

func (x *ELFLoaderCtx) GetDeployChecks() bool {
	if x != nil {
		return x.DeployChecks
	}
	return false
}

// Captures the results of a elf binary load.
// Structurally similar to fd_sbpf_program_t
type ELFLoaderEffects struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Rodata   []byte `protobuf:"bytes,1,opt,name=rodata,proto3" json:"rodata,omitempty"`
	RodataSz uint64 `protobuf:"varint,2,opt,name=rodata_sz,json=rodataSz,proto3" json:"rodata_sz,omitempty"`
	// bytes text = 3; // not needed, just points to a region in rodata
	TextCnt   uint64   `protobuf:"varint,4,opt,name=text_cnt,json=textCnt,proto3" json:"text_cnt,omitempty"`
	TextOff   uint64   `protobuf:"varint,5,opt,name=text_off,json=textOff,proto3" json:"text_off,omitempty"`
	EntryPc   uint64   `protobuf:"varint,6,opt,name=entry_pc,json=entryPc,proto3" json:"entry_pc,omitempty"`
	Calldests []uint64 `protobuf:"varint,7,rep,packed,name=calldests,proto3" json:"calldests,omitempty"`
}

func (x *ELFLoaderEffects) Reset() {
	*x = ELFLoaderEffects{}
	if protoimpl.UnsafeEnabled {
		mi := &file_elf_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ELFLoaderEffects) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ELFLoaderEffects) ProtoMessage() {}

func (x *ELFLoaderEffects) ProtoReflect() protoreflect.Message {
	mi := &file_elf_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ELFLoaderEffects.ProtoReflect.Descriptor instead.
func (*ELFLoaderEffects) Descriptor() ([]byte, []int) {
	return file_elf_proto_rawDescGZIP(), []int{2}
}

func (x *ELFLoaderEffects) GetRodata() []byte {
	if x != nil {
		return x.Rodata
	}
	return nil
}

func (x *ELFLoaderEffects) GetRodataSz() uint64 {
	if x != nil {
		return x.RodataSz
	}
	return 0
}

func (x *ELFLoaderEffects) GetTextCnt() uint64 {
	if x != nil {
		return x.TextCnt
	}
	return 0
}

func (x *ELFLoaderEffects) GetTextOff() uint64 {
	if x != nil {
		return x.TextOff
	}
	return 0
}

func (x *ELFLoaderEffects) GetEntryPc() uint64 {
	if x != nil {
		return x.EntryPc
	}
	return 0
}

func (x *ELFLoaderEffects) GetCalldests() []uint64 {
	if x != nil {
		return x.Calldests
	}
	return nil
}

type ELFLoaderFixture struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Input  *ELFLoaderCtx     `protobuf:"bytes,1,opt,name=input,proto3" json:"input,omitempty"`
	Output *ELFLoaderEffects `protobuf:"bytes,2,opt,name=output,proto3" json:"output,omitempty"`
}

func (x *ELFLoaderFixture) Reset() {
	*x = ELFLoaderFixture{}
	if protoimpl.UnsafeEnabled {
		mi := &file_elf_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ELFLoaderFixture) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ELFLoaderFixture) ProtoMessage() {}

func (x *ELFLoaderFixture) ProtoReflect() protoreflect.Message {
	mi := &file_elf_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ELFLoaderFixture.ProtoReflect.Descriptor instead.
func (*ELFLoaderFixture) Descriptor() ([]byte, []int) {
	return file_elf_proto_rawDescGZIP(), []int{3}
}

func (x *ELFLoaderFixture) GetInput() *ELFLoaderCtx {
	if x != nil {
		return x.Input
	}
	return nil
}

func (x *ELFLoaderFixture) GetOutput() *ELFLoaderEffects {
	if x != nil {
		return x.Output
	}
	return nil
}

var File_elf_proto protoreflect.FileDescriptor

var file_elf_proto_rawDesc = []byte{
	0x0a, 0x09, 0x65, 0x6c, 0x66, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x16, 0x6f, 0x72, 0x67,
	0x2e, 0x73, 0x6f, 0x6c, 0x61, 0x6e, 0x61, 0x2e, 0x73, 0x65, 0x61, 0x6c, 0x65, 0x76, 0x65, 0x6c,
	0x2e, 0x76, 0x31, 0x1a, 0x0d, 0x63, 0x6f, 0x6e, 0x74, 0x65, 0x78, 0x74, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x22, 0x1f, 0x0a, 0x09, 0x45, 0x4c, 0x46, 0x42, 0x69, 0x6e, 0x61, 0x72, 0x79, 0x12,
	0x12, 0x0a, 0x04, 0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x64,
	0x61, 0x74, 0x61, 0x22, 0xbf, 0x01, 0x0a, 0x0c, 0x45, 0x4c, 0x46, 0x4c, 0x6f, 0x61, 0x64, 0x65,
	0x72, 0x43, 0x74, 0x78, 0x12, 0x33, 0x0a, 0x03, 0x65, 0x6c, 0x66, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x21, 0x2e, 0x6f, 0x72, 0x67, 0x2e, 0x73, 0x6f, 0x6c, 0x61, 0x6e, 0x61, 0x2e, 0x73,
	0x65, 0x61, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x4c, 0x46, 0x42, 0x69,
	0x6e, 0x61, 0x72, 0x79, 0x52, 0x03, 0x65, 0x6c, 0x66, 0x12, 0x3e, 0x0a, 0x08, 0x66, 0x65, 0x61,
	0x74, 0x75, 0x72, 0x65, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x22, 0x2e, 0x6f, 0x72,
	0x67, 0x2e, 0x73, 0x6f, 0x6c, 0x61, 0x6e, 0x61, 0x2e, 0x73, 0x65, 0x61, 0x6c, 0x65, 0x76, 0x65,
	0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x46, 0x65, 0x61, 0x74, 0x75, 0x72, 0x65, 0x53, 0x65, 0x74, 0x52,
	0x08, 0x66, 0x65, 0x61, 0x74, 0x75, 0x72, 0x65, 0x73, 0x12, 0x15, 0x0a, 0x06, 0x65, 0x6c, 0x66,
	0x5f, 0x73, 0x7a, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x52, 0x05, 0x65, 0x6c, 0x66, 0x53, 0x7a,
	0x12, 0x23, 0x0a, 0x0d, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x5f, 0x63, 0x68, 0x65, 0x63, 0x6b,
	0x73, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x52, 0x0c, 0x64, 0x65, 0x70, 0x6c, 0x6f, 0x79, 0x43,
	0x68, 0x65, 0x63, 0x6b, 0x73, 0x22, 0xb6, 0x01, 0x0a, 0x10, 0x45, 0x4c, 0x46, 0x4c, 0x6f, 0x61,
	0x64, 0x65, 0x72, 0x45, 0x66, 0x66, 0x65, 0x63, 0x74, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x72, 0x6f,
	0x64, 0x61, 0x74, 0x61, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x06, 0x72, 0x6f, 0x64, 0x61,
	0x74, 0x61, 0x12, 0x1b, 0x0a, 0x09, 0x72, 0x6f, 0x64, 0x61, 0x74, 0x61, 0x5f, 0x73, 0x7a, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x04, 0x52, 0x08, 0x72, 0x6f, 0x64, 0x61, 0x74, 0x61, 0x53, 0x7a, 0x12,
	0x19, 0x0a, 0x08, 0x74, 0x65, 0x78, 0x74, 0x5f, 0x63, 0x6e, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x04, 0x52, 0x07, 0x74, 0x65, 0x78, 0x74, 0x43, 0x6e, 0x74, 0x12, 0x19, 0x0a, 0x08, 0x74, 0x65,
	0x78, 0x74, 0x5f, 0x6f, 0x66, 0x66, 0x18, 0x05, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x74, 0x65,
	0x78, 0x74, 0x4f, 0x66, 0x66, 0x12, 0x19, 0x0a, 0x08, 0x65, 0x6e, 0x74, 0x72, 0x79, 0x5f, 0x70,
	0x63, 0x18, 0x06, 0x20, 0x01, 0x28, 0x04, 0x52, 0x07, 0x65, 0x6e, 0x74, 0x72, 0x79, 0x50, 0x63,
	0x12, 0x1c, 0x0a, 0x09, 0x63, 0x61, 0x6c, 0x6c, 0x64, 0x65, 0x73, 0x74, 0x73, 0x18, 0x07, 0x20,
	0x03, 0x28, 0x04, 0x52, 0x09, 0x63, 0x61, 0x6c, 0x6c, 0x64, 0x65, 0x73, 0x74, 0x73, 0x22, 0x90,
	0x01, 0x0a, 0x10, 0x45, 0x4c, 0x46, 0x4c, 0x6f, 0x61, 0x64, 0x65, 0x72, 0x46, 0x69, 0x78, 0x74,
	0x75, 0x72, 0x65, 0x12, 0x3a, 0x0a, 0x05, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x18, 0x01, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x24, 0x2e, 0x6f, 0x72, 0x67, 0x2e, 0x73, 0x6f, 0x6c, 0x61, 0x6e, 0x61, 0x2e,
	0x73, 0x65, 0x61, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x4c, 0x46, 0x4c,
	0x6f, 0x61, 0x64, 0x65, 0x72, 0x43, 0x74, 0x78, 0x52, 0x05, 0x69, 0x6e, 0x70, 0x75, 0x74, 0x12,
	0x40, 0x0a, 0x06, 0x6f, 0x75, 0x74, 0x70, 0x75, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
	0x28, 0x2e, 0x6f, 0x72, 0x67, 0x2e, 0x73, 0x6f, 0x6c, 0x61, 0x6e, 0x61, 0x2e, 0x73, 0x65, 0x61,
	0x6c, 0x65, 0x76, 0x65, 0x6c, 0x2e, 0x76, 0x31, 0x2e, 0x45, 0x4c, 0x46, 0x4c, 0x6f, 0x61, 0x64,
	0x65, 0x72, 0x45, 0x66, 0x66, 0x65, 0x63, 0x74, 0x73, 0x52, 0x06, 0x6f, 0x75, 0x74, 0x70, 0x75,
	0x74, 0x42, 0x0f, 0x5a, 0x0d, 0x2e, 0x2f, 0x63, 0x6f, 0x6e, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x6e,
	0x63, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_elf_proto_rawDescOnce sync.Once
	file_elf_proto_rawDescData = file_elf_proto_rawDesc
)

func file_elf_proto_rawDescGZIP() []byte {
	file_elf_proto_rawDescOnce.Do(func() {
		file_elf_proto_rawDescData = protoimpl.X.CompressGZIP(file_elf_proto_rawDescData)
	})
	return file_elf_proto_rawDescData
}

var file_elf_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_elf_proto_goTypes = []any{
	(*ELFBinary)(nil),        // 0: org.solana.sealevel.v1.ELFBinary
	(*ELFLoaderCtx)(nil),     // 1: org.solana.sealevel.v1.ELFLoaderCtx
	(*ELFLoaderEffects)(nil), // 2: org.solana.sealevel.v1.ELFLoaderEffects
	(*ELFLoaderFixture)(nil), // 3: org.solana.sealevel.v1.ELFLoaderFixture
	(*FeatureSet)(nil),       // 4: org.solana.sealevel.v1.FeatureSet
}
var file_elf_proto_depIdxs = []int32{
	0, // 0: org.solana.sealevel.v1.ELFLoaderCtx.elf:type_name -> org.solana.sealevel.v1.ELFBinary
	4, // 1: org.solana.sealevel.v1.ELFLoaderCtx.features:type_name -> org.solana.sealevel.v1.FeatureSet
	1, // 2: org.solana.sealevel.v1.ELFLoaderFixture.input:type_name -> org.solana.sealevel.v1.ELFLoaderCtx
	2, // 3: org.solana.sealevel.v1.ELFLoaderFixture.output:type_name -> org.solana.sealevel.v1.ELFLoaderEffects
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_elf_proto_init() }
func file_elf_proto_init() {
	if File_elf_proto != nil {
		return
	}
	file_context_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_elf_proto_msgTypes[0].Exporter = func(v any, i int) any {
			switch v := v.(*ELFBinary); i {
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
		file_elf_proto_msgTypes[1].Exporter = func(v any, i int) any {
			switch v := v.(*ELFLoaderCtx); i {
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
		file_elf_proto_msgTypes[2].Exporter = func(v any, i int) any {
			switch v := v.(*ELFLoaderEffects); i {
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
		file_elf_proto_msgTypes[3].Exporter = func(v any, i int) any {
			switch v := v.(*ELFLoaderFixture); i {
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
			RawDescriptor: file_elf_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_elf_proto_goTypes,
		DependencyIndexes: file_elf_proto_depIdxs,
		MessageInfos:      file_elf_proto_msgTypes,
	}.Build()
	File_elf_proto = out.File
	file_elf_proto_rawDesc = nil
	file_elf_proto_goTypes = nil
	file_elf_proto_depIdxs = nil
}
