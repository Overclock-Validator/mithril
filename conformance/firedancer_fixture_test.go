package conformance

import (
	"encoding/binary"

	legacyproto "github.com/golang/protobuf/proto"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

type firedancerFeatureSet struct {
	Features []uint64 `protobuf:"fixed64,1,rep,packed,name=features,proto3" json:"features,omitempty"`
}

func (x *firedancerFeatureSet) Reset()         { *x = firedancerFeatureSet{} }
func (x *firedancerFeatureSet) String() string { return legacyproto.CompactTextString(x) }
func (*firedancerFeatureSet) ProtoMessage()    {}

type firedancerCurrentELFLoaderCtx struct {
	ElfData      []byte                `protobuf:"bytes,1,opt,name=elf_data,json=elfData,proto3" json:"elf_data,omitempty"`
	Features     *firedancerFeatureSet `protobuf:"bytes,2,opt,name=features,proto3" json:"features,omitempty"`
	DeployChecks bool                  `protobuf:"varint,3,opt,name=deploy_checks,json=deployChecks,proto3" json:"deploy_checks,omitempty"`
}

func (x *firedancerCurrentELFLoaderCtx) Reset()         { *x = firedancerCurrentELFLoaderCtx{} }
func (x *firedancerCurrentELFLoaderCtx) String() string { return legacyproto.CompactTextString(x) }
func (*firedancerCurrentELFLoaderCtx) ProtoMessage()    {}

type firedancerCurrentELFLoaderEffects struct {
	ErrCode       *uint32 `protobuf:"varint,1,opt,name=err_code,json=errCode" json:"err_code,omitempty"`
	RodataHash    *uint64 `protobuf:"fixed64,2,opt,name=rodata_hash,json=rodataHash" json:"rodata_hash,omitempty"`
	TextCnt       *uint64 `protobuf:"varint,3,opt,name=text_cnt,json=textCnt" json:"text_cnt,omitempty"`
	TextOff       *uint64 `protobuf:"varint,4,opt,name=text_off,json=textOff" json:"text_off,omitempty"`
	EntryPc       *uint64 `protobuf:"varint,5,opt,name=entry_pc,json=entryPc" json:"entry_pc,omitempty"`
	CalldestsHash *uint64 `protobuf:"fixed64,6,opt,name=calldests_hash,json=calldestsHash" json:"calldests_hash,omitempty"`
}

func (x *firedancerCurrentELFLoaderEffects) Reset() {
	*x = firedancerCurrentELFLoaderEffects{}
}
func (x *firedancerCurrentELFLoaderEffects) String() string {
	return legacyproto.CompactTextString(x)
}
func (*firedancerCurrentELFLoaderEffects) ProtoMessage() {}

type firedancerCurrentELFLoaderFixture struct {
	Input  *firedancerCurrentELFLoaderCtx     `protobuf:"bytes,2,opt,name=input,proto3" json:"input,omitempty"`
	Output *firedancerCurrentELFLoaderEffects `protobuf:"bytes,3,opt,name=output,proto3" json:"output,omitempty"`
}

func (x *firedancerCurrentELFLoaderFixture) Reset() {
	*x = firedancerCurrentELFLoaderFixture{}
}
func (x *firedancerCurrentELFLoaderFixture) String() string {
	return legacyproto.CompactTextString(x)
}
func (*firedancerCurrentELFLoaderFixture) ProtoMessage() {}

type firedancerELFLoaderFixtureCompat struct {
	ElfData      []byte
	Features     *FeatureSet
	DeployChecks bool
	Output       *firedancerELFLoaderOutputCompat
}

type firedancerELFLoaderOutputCompat struct {
	ErrCode       uint32
	HasErrCode    bool
	TextCnt       uint64
	HasTextCnt    bool
	TextOff       uint64
	HasTextOff    bool
	EntryPc       uint64
	HasEntryPc    bool
	RodataHash    uint64
	HasRodataHash bool
	CalldestsHash uint64
	HasCallHash   bool
}

func (o *firedancerELFLoaderOutputCompat) expectsSuccess() bool {
	if o == nil {
		return false
	}
	return !o.HasErrCode || o.ErrCode == 0
}

type firedancerInstrFixture struct {
	Input  *InstrContext `protobuf:"bytes,2,opt,name=input,proto3" json:"input,omitempty"`
	Output *InstrEffects `protobuf:"bytes,3,opt,name=output,proto3" json:"output,omitempty"`
}

func (x *firedancerInstrFixture) Reset()         { *x = firedancerInstrFixture{} }
func (x *firedancerInstrFixture) String() string { return legacyproto.CompactTextString(x) }
func (*firedancerInstrFixture) ProtoMessage()    {}

func unmarshalFiredancerELFLoaderFixture(data []byte) (*firedancerELFLoaderFixtureCompat, error) {
	fixture := &ELFLoaderFixture{}
	if err := proto.Unmarshal(data, fixture); err == nil && isELFData(fixture.GetInput().GetElf().GetData()) {
		var output *firedancerELFLoaderOutputCompat
		if fixture.GetOutput() != nil {
			output = &firedancerELFLoaderOutputCompat{
				TextCnt:    fixture.GetOutput().GetTextCnt(),
				HasTextCnt: true,
				TextOff:    fixture.GetOutput().GetTextOff(),
				HasTextOff: true,
				EntryPc:    fixture.GetOutput().GetEntryPc(),
				HasEntryPc: true,
			}
		}
		return &firedancerELFLoaderFixtureCompat{
			ElfData:      fixture.GetInput().GetElf().GetData(),
			Features:     fixture.GetInput().GetFeatures(),
			DeployChecks: fixture.GetInput().GetDeployChecks(),
			Output:       output,
		}, nil
	}

	currentFixture := &firedancerCurrentELFLoaderFixture{}
	if err := legacyproto.Unmarshal(data, currentFixture); err != nil {
		return nil, err
	}
	if currentFixture.Input == nil {
		return nil, legacyproto.ErrNil
	}
	var features *FeatureSet
	if currentFixture.Input != nil && currentFixture.Input.Features != nil {
		features = &FeatureSet{Features: currentFixture.Input.Features.Features}
	}
	var output *firedancerELFLoaderOutputCompat
	if currentFixture.Output != nil {
		output = &firedancerELFLoaderOutputCompat{}
		if currentFixture.Output.ErrCode != nil {
			output.ErrCode = *currentFixture.Output.ErrCode
			output.HasErrCode = true
		}
		if currentFixture.Output.TextCnt != nil {
			output.TextCnt = *currentFixture.Output.TextCnt
			output.HasTextCnt = true
		}
		if currentFixture.Output.TextOff != nil {
			output.TextOff = *currentFixture.Output.TextOff
			output.HasTextOff = true
		}
		if currentFixture.Output.EntryPc != nil {
			output.EntryPc = *currentFixture.Output.EntryPc
			output.HasEntryPc = true
		}
		if currentFixture.Output.RodataHash != nil {
			output.RodataHash = *currentFixture.Output.RodataHash
			output.HasRodataHash = true
		}
		if currentFixture.Output.CalldestsHash != nil {
			output.CalldestsHash = *currentFixture.Output.CalldestsHash
			output.HasCallHash = true
		}
	}
	return &firedancerELFLoaderFixtureCompat{
		ElfData:      currentFixture.Input.ElfData,
		Features:     features,
		DeployChecks: currentFixture.Input.DeployChecks,
		Output:       output,
	}, nil
}

func isELFData(data []byte) bool {
	return len(data) >= 4 && data[0] == 0x7f && data[1] == 'E' && data[2] == 'L' && data[3] == 'F'
}

func unmarshalFiredancerInstrFixture(data []byte) (*InstrFixture, error) {
	fixture := &InstrFixture{}
	if err := proto.Unmarshal(data, fixture); err == nil && len(fixture.GetInput().GetProgramId()) == 32 {
		return fixture, nil
	}

	currentFixture := &firedancerInstrFixture{}
	if err := legacyproto.Unmarshal(data, currentFixture); err != nil {
		return nil, err
	}
	if currentFixture.Input == nil {
		return nil, legacyproto.ErrNil
	}
	features, ok := currentInstrFeatures(data)
	if ok {
		// protosol v5.4.0 flattened epoch_context away: InstrContext now
		// carries the FeatureSet directly at field 10.
		currentFixture.Input.Features = &FeatureSet{Features: features}
	} else if currentFixture.Input.Features == nil {
		currentFixture.Input.Features = &FeatureSet{}
	}
	// slot_context was removed from InstrContext in protosol v5.4.0.
	return &InstrFixture{
		Input:  currentFixture.Input,
		Output: currentFixture.Output,
	}, nil
}

func currentInstrFeatures(data []byte) ([]uint64, bool) {
	input, ok := consumeBytesField(data, 2)
	if !ok {
		return nil, false
	}
	epochContext, ok := consumeBytesField(input, 10)
	if !ok {
		return nil, false
	}
	featureSet, ok := consumeBytesField(epochContext, 1)
	if !ok {
		return nil, false
	}
	return consumePackedFeatureIds(featureSet), true
}

func consumeBytesField(data []byte, want protowire.Number) ([]byte, bool) {
	for len(data) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return nil, false
		}
		data = data[tagLen:]
		if typ == protowire.BytesType {
			value, valueLen := protowire.ConsumeBytes(data)
			if valueLen < 0 {
				return nil, false
			}
			if num == want {
				return value, true
			}
			data = data[valueLen:]
			continue
		}
		valueLen := protowire.ConsumeFieldValue(num, typ, data)
		if valueLen < 0 {
			return nil, false
		}
		data = data[valueLen:]
	}
	return nil, false
}

func consumeFeatureIds(data []byte) []uint64 {
	var featureIds []uint64
	for len(data) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(data)
		if tagLen < 0 {
			return featureIds
		}
		data = data[tagLen:]
		if num != 1 {
			valueLen := protowire.ConsumeFieldValue(num, typ, data)
			if valueLen < 0 {
				return featureIds
			}
			data = data[valueLen:]
			continue
		}
		switch typ {
		case protowire.VarintType:
			value, valueLen := protowire.ConsumeVarint(data)
			if valueLen < 0 {
				return featureIds
			}
			featureIds = append(featureIds, value)
			data = data[valueLen:]
		case protowire.Fixed64Type:
			value, valueLen := protowire.ConsumeFixed64(data)
			if valueLen < 0 {
				return featureIds
			}
			featureIds = append(featureIds, value)
			data = data[valueLen:]
		case protowire.BytesType:
			value, valueLen := protowire.ConsumeBytes(data)
			if valueLen < 0 {
				return featureIds
			}
			featureIds = append(featureIds, consumePackedFeatureIds(value)...)
			data = data[valueLen:]
		default:
			valueLen := protowire.ConsumeFieldValue(num, typ, data)
			if valueLen < 0 {
				return featureIds
			}
			data = data[valueLen:]
		}
	}
	return featureIds
}

func consumePackedFeatureIds(data []byte) []uint64 {
	var featureIds []uint64
	if len(data)%8 == 0 {
		for len(data) > 0 {
			featureIds = append(featureIds, binary.LittleEndian.Uint64(data[:8]))
			data = data[8:]
		}
		return featureIds
	}

	remaining := data
	for len(remaining) > 0 {
		value, valueLen := protowire.ConsumeVarint(remaining)
		if valueLen < 0 {
			featureIds = featureIds[:0]
			break
		}
		featureIds = append(featureIds, value)
		remaining = remaining[valueLen:]
	}
	return featureIds
}
