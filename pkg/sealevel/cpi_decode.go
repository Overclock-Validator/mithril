package sealevel

import "encoding/binary"

// These helpers decode complete records only, after VM.Translate validates the
// entire range. General io.Reader decoding remains in the Unmarshal methods.
func decodeAccountInfoC(accountInfo *SolAccountInfoC, acctInfoBytes []byte) {
	_ = acctInfoBytes[55]
	accountInfo.KeyAddr = binary.LittleEndian.Uint64(acctInfoBytes[:8])
	accountInfo.LamportsAddr = binary.LittleEndian.Uint64(acctInfoBytes[8:16])
	accountInfo.DataLen = binary.LittleEndian.Uint64(acctInfoBytes[16:24])
	accountInfo.DataAddr = binary.LittleEndian.Uint64(acctInfoBytes[24:32])
	accountInfo.OwnerAddr = binary.LittleEndian.Uint64(acctInfoBytes[32:40])
	accountInfo.RentEpoch = binary.LittleEndian.Uint64(acctInfoBytes[40:48])
	accountInfo.IsSigner = acctInfoBytes[48] != 0
	accountInfo.IsWritable = acctInfoBytes[49] != 0
	accountInfo.Executable = acctInfoBytes[50] != 0

}
func decodeAccountInfoRust(accountInfo *SolAccountInfoRust, accountInfoBytes []byte) {
	_ = accountInfoBytes[47]
	accountInfo.PubkeyAddr = binary.LittleEndian.Uint64(accountInfoBytes[:8])
	accountInfo.LamportsBoxAddr = binary.LittleEndian.Uint64(accountInfoBytes[8:16])
	accountInfo.DataBoxAddr = binary.LittleEndian.Uint64(accountInfoBytes[16:24])
	accountInfo.OwnerAddr = binary.LittleEndian.Uint64(accountInfoBytes[24:32])
	accountInfo.RentEpoch = binary.LittleEndian.Uint64(accountInfoBytes[32:40])
	accountInfo.IsSigner = accountInfoBytes[40]
	accountInfo.IsWritable = accountInfoBytes[41]
	accountInfo.Executable = accountInfoBytes[42]

}

// The VM has already validated the complete RefCell record.
func decodeRefCellRust(data []byte) RefCellRust {
	_ = data[RefCellRustSize-1]
	return RefCellRust{Strong: binary.LittleEndian.Uint64(data[:8]), Weak: binary.LittleEndian.Uint64(data[8:16]), Borrow: binary.LittleEndian.Uint64(data[16:24]), Addr: binary.LittleEndian.Uint64(data[24:32])}
}
func decodeRefCellVecRust(data []byte) RefCellVecRust {
	_ = data[RefCellVecRustSize-1]
	return RefCellVecRust{Strong: binary.LittleEndian.Uint64(data[:8]), Weak: binary.LittleEndian.Uint64(data[8:16]), Borrow: binary.LittleEndian.Uint64(data[16:24]), Addr: binary.LittleEndian.Uint64(data[24:32]), Len: binary.LittleEndian.Uint64(data[32:40])}
}
