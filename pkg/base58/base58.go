// Copyright 2022 Firedancer Contributors

// Package base58 converts between binary and Base58 for 32/64 length strings.
//
// Ported from Firedancer:
// https://github.com/firedancer-io/firedancer/blob/main/src/ballet/base58/fd_base58.h
//
// Original author: Philip Taffet <phtaffet@jumptrading.com>
package base58

import (
	"encoding/binary"
	"errors"
	"fmt"
)

var ErrEncode = errors.New("base58 encoding error")

// alphabet maps [0, 58) to the base58 character.
const alphabet = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"

const (
	// N = 32
	inter32sz = 9 // Computed by ceil(log_(58^5) (256^32-1))
	bin32sz   = 8 //  N / 4
	raw32sz   = inter32sz * 5

	// N = 64
	inter64sz = 18 // Computed by ceil(log_(58^5) (256^64-1))
	bin64sz   = 16 //  N / 4
	raw64sz   = inter64sz * 5
)

const r1Div uint64 = 656356768 // = 58^5

const (
	invalidChar        uint8 = 0xFF
	inverseLUTOffset   uint8 = '1'
	inverseLUTSentinel uint8 = 1 + 'z' - inverseLUTOffset
)

// inverseLUT maps (character value - '1') to [0, 58).
var inverseLUT = [75]byte{
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
	0xFF, 0x11, 0x12, 0x13, 0x14, 0x15, 0xFF, 0x16,
	0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E,
	0x1F, 0x20, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
	0x21, 0x22, 0x23, 0x24, 0x25, 0x26, 0x27, 0x28,
	0x29, 0x2A, 0x2B, 0xFF, 0x2C, 0x2D, 0x2E, 0x2F,
	0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37,
	0x38, 0x39, 0xFF,
}

// encTable32 contains the unique values less than 58^5 such that:
//
//	2^(32*(7-j)) = sum_k table[j][k]*58^(5*(7-k))
//
// The second dimension of this table is actually ceil(log_(58^5)
// (2^(32*7)), but that's almost always 8
var encTable32 = [bin32sz][inter32sz - 1]uint32{
	{513735, 77223048, 437087610, 300156666, 605448490, 214625350, 141436834, 379377856},
	{0, 78508, 646269101, 118408823, 91512303, 209184527, 413102373, 153715680},
	{0, 0, 11997, 486083817, 3737691, 294005210, 247894721, 289024608},
	{0, 0, 0, 1833, 324463681, 385795061, 551597588, 21339008},
	{0, 0, 0, 0, 280, 127692781, 389432875, 357132832},
	{0, 0, 0, 0, 0, 42, 537767569, 410450016},
	{0, 0, 0, 0, 0, 0, 6, 356826688},
	{0, 0, 0, 0, 0, 0, 0, 1},
}

// encTable64 Contains the unique values less than 58^5 such that
//
//	2^(32*(15-j)) = sum_k table[j][k]*58^(5*(16-k))
var encTable64 = [bin64sz][inter64sz - 1]uint32{
	{2631, 149457141, 577092685, 632289089, 81912456, 221591423, 502967496, 403284731, 377738089, 492128779, 746799, 366351977, 190199623, 38066284, 526403762, 650603058, 454901440},
	{0, 402, 68350375, 30641941, 266024478, 208884256, 571208415, 337765723, 215140626, 129419325, 480359048, 398051646, 635841659, 214020719, 136986618, 626219915, 49699360},
	{0, 0, 61, 295059608, 141201404, 517024870, 239296485, 527697587, 212906911, 453637228, 467589845, 144614682, 45134568, 184514320, 644355351, 104784612, 308625792},
	{0, 0, 0, 9, 256449755, 500124311, 479690581, 372802935, 413254725, 487877412, 520263169, 176791855, 78190744, 291820402, 74998585, 496097732, 59100544},
	{0, 0, 0, 0, 1, 285573662, 455976778, 379818553, 100001224, 448949512, 109507367, 117185012, 347328982, 522665809, 36908802, 577276849, 64504928},
	{0, 0, 0, 0, 0, 0, 143945778, 651677945, 281429047, 535878743, 264290972, 526964023, 199595821, 597442702, 499113091, 424550935, 458949280},
	{0, 0, 0, 0, 0, 0, 0, 21997789, 294590275, 148640294, 595017589, 210481832, 404203788, 574729546, 160126051, 430102516, 44963712},
	{0, 0, 0, 0, 0, 0, 0, 0, 3361701, 325788598, 30977630, 513969330, 194569730, 164019635, 136596846, 626087230, 503769920},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 513735, 77223048, 437087610, 300156666, 605448490, 214625350, 141436834, 379377856},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 78508, 646269101, 118408823, 91512303, 209184527, 413102373, 153715680},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 11997, 486083817, 3737691, 294005210, 247894721, 289024608},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1833, 324463681, 385795061, 551597588, 21339008},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 280, 127692781, 389432875, 357132832},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 42, 537767569, 410450016},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 6, 356826688},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
}

// decTable32 contains the unique values less than 2^32 such that:
//
//	58^(5*(8-j)) = sum_k table[j][k]*2^(32*(7-k))
var decTable32 = [inter32sz][bin32sz]uint32{
	{1277, 2650397687, 3801011509, 2074386530, 3248244966, 687255411, 2959155456, 0},
	{0, 8360, 1184754854, 3047609191, 3418394749, 132556120, 1199103528, 0},
	{0, 0, 54706, 2996985344, 1834629191, 3964963911, 485140318, 1073741824},
	{0, 0, 0, 357981, 1476998812, 3337178590, 1483338760, 4194304000},
	{0, 0, 0, 0, 2342503, 3052466824, 2595180627, 17825792},
	{0, 0, 0, 0, 0, 15328518, 1933902296, 4063920128},
	{0, 0, 0, 0, 0, 0, 100304420, 3355157504},
	{0, 0, 0, 0, 0, 0, 0, 656356768},
	{0, 0, 0, 0, 0, 0, 0, 1},
}

// decTable64 contains the unique values less than 2^32 such that:
var decTable64 = [inter64sz][bin64sz]uint32{
	{249448, 3719864065, 173911550, 4021557284, 3115810883, 2498525019, 1035889824, 627529458, 3840888383, 3728167192, 2901437456, 3863405776, 1540739182, 1570766848, 0, 0},
	{0, 1632305, 1882780341, 4128706713, 1023671068, 2618421812, 2005415586, 1062993857, 3577221846, 3960476767, 1695615427, 2597060712, 669472826, 104923136, 0, 0},
	{0, 0, 10681231, 1422956801, 2406345166, 4058671871, 2143913881, 4169135587, 2414104418, 2549553452, 997594232, 713340517, 2290070198, 1103833088, 0, 0},
	{0, 0, 0, 69894212, 1038812943, 1785020643, 1285619000, 2301468615, 3492037905, 314610629, 2761740102, 3410618104, 1699516363, 910779968, 0, 0},
	{0, 0, 0, 0, 457363084, 927569770, 3976106370, 1389513021, 2107865525, 3716679421, 1828091393, 2088408376, 439156799, 2579227194, 0, 0},
	{0, 0, 0, 0, 0, 2992822783, 383623235, 3862831115, 112778334, 339767049, 1447250220, 486575164, 3495303162, 2209946163, 268435456, 0},
	{0, 0, 0, 0, 0, 4, 2404108010, 2962826229, 3998086794, 1893006839, 2266258239, 1429430446, 307953032, 2361423716, 176160768, 0},
	{0, 0, 0, 0, 0, 0, 29, 3596590989, 3044036677, 1332209423, 1014420882, 868688145, 4264082837, 3688771808, 2485387264, 0},
	{0, 0, 0, 0, 0, 0, 0, 195, 1054003707, 3711696540, 582574436, 3549229270, 1088536814, 2338440092, 1468637184, 0},
	{0, 0, 0, 0, 0, 0, 0, 0, 1277, 2650397687, 3801011509, 2074386530, 3248244966, 687255411, 2959155456, 0},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 8360, 1184754854, 3047609191, 3418394749, 132556120, 1199103528, 0},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 54706, 2996985344, 1834629191, 3964963911, 485140318, 1073741824},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 357981, 1476998812, 3337178590, 1483338760, 4194304000},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2342503, 3052466824, 2595180627, 17825792},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 15328518, 1933902296, 4063920128},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 100304420, 3355157504},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 656356768},
	{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1},
}

func Encode32(out *[44]byte, in [32]byte) uint {
	const raw58sz = 45

	// Count leading zeros (needed for final output)
	var inLeading0s uint
	for i := range in {
		if in[i] != 0 {
			break
		}
		inLeading0s++
	}

	// X = sum_i bytes[i] * 2^(8*(32-1-i))

	// Convert N to 32-bit limbs:
	// X = sum_i binary[i] * 2^(32*(8-1-i))
	var limbs [8]uint32
	for i := range limbs {
		limbs[i] = binary.BigEndian.Uint32(in[4*i:])
	}

	// Convert to the intermediate format:
	//   X = sum_i intermediate[i] * 58^(5*(INTERMEDIATE_SZ-1-i))
	// Initially, we don't require intermediate[i] < 58^5,
	// but we do want to make sure the sums don't overflow.

	var intermediate [9]uint64

	// The worst case is if binary[7] is (2^32)-1. In that case
	// intermediate[8] will be be just over 2^63, which is fine.

	for i := 0; i < bin32sz; i++ {
		for j := 0; j < 8; j++ {
			intermediate[j+1] += uint64(limbs[i]) * uint64(encTable32[i][j])
		}
	}

	// Now we make sure each term is less than 58^5.
	// Again, we have to be a bit careful of overflow.
	//
	// For N==32, in the worst case, as before, intermediate[8] will be
	// just over 2^63 and intermediate[7] will be just over 2^62.6.  In
	// the first step, we'll add floor(intermediate[8]/58^5) to
	// intermediate[7].  58^5 is pretty big though, so intermediate[7]
	// barely budges, and this is still fine.
	//
	// For N==64, in the worst case, the biggest entry in intermediate
	// at this point is 2^63.87, and in the worst case, we add
	// (2^64-1)/58^5, which is still about 2^63.87.

	for i := 8; i > 0; i-- {
		intermediate[i-1] += intermediate[i] / r1Div
		intermediate[i] %= r1Div
	}

	// Convert intermediate form to base 58.
	//   X = sum_i raw_base58[i] * 58^(RAW58_SZ-1-i)

	var rawBase58 [raw32sz]byte
	for i := 0; i < inter32sz; i++ {
		// We know intermediate[ i ] < 58^5 < 2^32 for all i, so casting
		// to a uint32 is safe.
		v := uint32(intermediate[i])
		rawBase58[5*i+4] = byte((v / 1) % 58)
		rawBase58[5*i+3] = byte((v / 58) % 58)
		rawBase58[5*i+2] = byte((v / 3364) % 58)
		rawBase58[5*i+1] = byte((v / 195112) % 58)
		// We know this one is less than 58
		rawBase58[5*i+0] = byte(v / 11316496)
	}

	// Finally, actually convert to the string.
	// We have to ignore all the leading zeros in rawBase58 and instead
	// insert inLeading0s leading '1' characters.  We can show that
	// rawBase58 actually has at least inLeading0s, so we'll do this
	// by skipping the first few leading zeros in rawBase58.

	var rawLeading0s uint
	for rawLeading0s = 0; rawLeading0s < raw32sz; rawLeading0s++ {
		if rawBase58[rawLeading0s] != 0 {
			break
		}
	}

	// It's not immediately obvious that rawLeading0s >= inLeading0s,
	// but it's true.  In base b, X has floor(log_b X)+1 digits.  That
	// means inLeading0s = N-1-floor(log_256 X) and rawLeading0s =
	// RAW58_SZ-1-floor(log_58 X).  Let X<256^N be given and consider:
	//
	//   rawLeading0s - inLeading0s =
	//     =  RAW58_SZ-N + floor( log_256 X ) - floor( log_58 X )
	//     >= RAW58_SZ-N - 1 + ( log_256 X - log_58 X ) .
	//
	// log_256 X - log_58 X is monotonically decreasing for X>0, so it
	// achieves it minimum at the maximum possible value for X, i.e.
	// 256^N-1.
	//   >= RAW58_SZ-N-1 + log_256(256^N-1) - log_58(256^N-1)
	//
	// When N==32, RAW58_SZ is 45, so this gives skip >= 0.29
	// When N==64, RAW58_SZ is 90, so this gives skip >= 1.59.
	//
	// Regardless, rawLeading0s - inLeading0s >= 0.

	skip := rawLeading0s - inLeading0s
	for i := uint(0); i < raw32sz-skip; i++ {
		out[i] = alphabet[rawBase58[i+skip]]
	}

	return raw32sz - skip
}

func Encode64(out *[88]byte, in [64]byte) uint {
	// Count leading zeros (needed for final output)
	var inLeading0s uint
	for i := range in {
		if in[i] != 0 {
			break
		}
		inLeading0s++
	}

	// X = sum_i bytes[i] * 2^(8*(64-1-i))

	// Convert N to 32-bit limbs:
	// X = sum_i binary[i] * 2^(32*(16-1-i))
	var limbs [16]uint32
	for i := range limbs {
		limbs[i] = binary.BigEndian.Uint32(in[4*i:])
	}

	// Convert to the intermediate format:
	//   X = sum_i intermediate[i] * 58^(5*(INTERMEDIATE_SZ-1-i))
	// Initially, we don't require intermediate[i] < 58^5,
	// but we do want to make sure the sums don't overflow.

	var intermediate [inter64sz]uint64

	// If we do it the same way as the 32B conversion, intermediate[16]
	// can overflow when the input is sufficiently large.  We'll do a
	// mini-reduction after the first 8 steps.  After the first 8 terms,
	// the largest intermediate[16] can be is 2^63.87.  Then, after
	// reduction it'll be at most 58^5, and after adding the last terms,
	// it won't exceed 2^63.1.  We do need to be cautious that the
	// mini-reduction doesn't cause overflow in intermediate[15] though.
	// Pre-mini-reduction, it's at most 2^63.05.  The mini-reduction adds
	// at most 2^64/58^5, which is negligible.  With the final terms, it
	// won't exceed 2^63.69, which is fine. Other terms are less than
	// 2^63.76, so no problems there.

	for i := 0; i < 8; i++ {
		for j := 0; j < inter64sz-1; j++ {
			intermediate[j+1] += uint64(limbs[i]) * uint64(encTable64[i][j])
		}
	}

	//  Mini-reduction
	intermediate[15] += intermediate[16] / r1Div
	intermediate[16] %= r1Div

	// Finish iterations
	for i := 8; i < bin64sz; i++ {
		for j := 0; j < inter64sz-1; j++ {
			intermediate[j+1] += uint64(limbs[i]) * uint64(encTable64[i][j])
		}
	}

	for i := inter64sz - 1; i > 0; i-- {
		intermediate[i-1] += intermediate[i] / r1Div
		intermediate[i] %= r1Div
	}

	// Convert intermediate form to base 58.
	//   X = sum_i raw_base58[i] * 58^(RAW58_SZ-1-i)

	var rawBase58 [raw64sz]byte
	for i := 0; i < inter64sz; i++ {
		// We know intermediate[ i ] < 58^5 < 2^32 for all i, so casting
		// to a uint32 is safe.
		v := uint32(intermediate[i])
		rawBase58[5*i+4] = byte((v / 1) % 58)
		rawBase58[5*i+3] = byte((v / 58) % 58)
		rawBase58[5*i+2] = byte((v / 3364) % 58)
		rawBase58[5*i+1] = byte((v / 195112) % 58)
		// We know this one is less than 58
		rawBase58[5*i+0] = byte(v / 11316496)
	}

	// Finally, actually convert to the string.
	// We have to ignore all the leading zeros in rawBase58 and instead
	// insert inLeading0s leading '1' characters.  We can show that
	// rawBase58 actually has at least inLeading0s, so we'll do this
	// by skipping the first few leading zeros in rawBase58.

	var rawLeading0s uint
	for rawLeading0s = 0; rawLeading0s < raw64sz; rawLeading0s++ {
		if rawBase58[rawLeading0s] != 0 {
			break
		}
	}

	// It's not immediately obvious that rawLeading0s >= inLeading0s,
	// but it's true.  In base b, X has floor(log_b X)+1 digits.  That
	// means inLeading0s = N-1-floor(log_256 X) and rawLeading0s =
	// RAW58_SZ-1-floor(log_58 X).  Let X<256^N be given and consider:
	//
	//   rawLeading0s - inLeading0s =
	//     =  RAW58_SZ-N + floor( log_256 X ) - floor( log_58 X )
	//     >= RAW58_SZ-N - 1 + ( log_256 X - log_58 X ) .
	//
	// log_256 X - log_58 X is monotonically decreasing for X>0, so it
	// achieves it minimum at the maximum possible value for X, i.e.
	// 256^N-1.
	//   >= RAW58_SZ-N-1 + log_256(256^N-1) - log_58(256^N-1)
	//
	// When N==32, RAW58_SZ is 45, so this gives skip >= 0.29
	// When N==64, RAW58_SZ is 90, so this gives skip >= 1.59.
	//
	// Regardless, rawLeading0s - inLeading0s >= 0.

	skip := rawLeading0s - inLeading0s
	for i := uint(0); i < raw64sz-skip; i++ {
		out[i] = alphabet[rawBase58[i+skip]]
	}

	return raw64sz - skip
}

func Decode32(out *[32]byte, encoded []byte) (ok bool) {
	// Check length
	if len(encoded) < 32 || len(encoded) > 44 {
		return false
	}

	// Validate string
	for _, c := range encoded {
		idx := int(c) - int(inverseLUTOffset)
		if idx < 0 || idx > int(inverseLUTSentinel) {
			idx = int(inverseLUTSentinel)
		}
		if inverseLUT[idx] == invalidChar {
			return false
		}
	}

	// X = sum_i raw_base58[i] * 58^(RAW58_SZ-1-i)
	var rawBase58 [raw32sz]byte

	// Prepend enough 0s to make it exactly RAW58_SZ characters
	prepend0 := raw32sz - len(encoded)
	for j := 0; j < raw32sz; j++ {
		if j >= int(prepend0) {
			rawBase58[j] = inverseLUT[encoded[j-int(prepend0)]-inverseLUTOffset]
		}
	}

	// Convert to the intermediate format
	//   X = sum_i intermediate[i] * 58^(5*(INTERMEDIATE_SZ-1-i))
	var intermediate [inter32sz]uint64
	for i := 0; i < inter32sz; i++ {
		intermediate[i] = uint64(rawBase58[5*i+0])*11316496 +
			uint64(rawBase58[5*i+1])*195112 +
			uint64(rawBase58[5*i+2])*3364 +
			uint64(rawBase58[5*i+3])*58 +
			uint64(rawBase58[5*i+4])
	}

	// Using the table, convert to overcomplete base 2^32 (terms can be
	// larger than 2^32).  We need to be careful about overflow.
	//
	// For N==32, the largest anything in binary can get is binary[7]:
	// even if intermediate[i]==58^5-1 for all i, then binary[7] < 2^63.
	var binary_ [bin32sz]uint64
	for j := 0; j < bin32sz; j++ {
		var acc uint64
		for i := 0; i < inter32sz; i++ {
			acc += uint64(intermediate[i]) * uint64(decTable32[i][j])
		}
		binary_[j] = acc
	}

	// Make sure each term is less than 2^32.
	//
	// For N==32, we have plenty of headroom in binary, so overflow is
	// not a concern this time.
	for i := bin32sz - 1; i > 0; i-- {
		binary_[i-1] += binary_[i] >> 32
		binary_[i] &= 0xFFFFFFFF
	}

	// If the largest term is 2^32 or bigger, it means N is larger than
	// what can fit in BYTE_CNT bytes.  This can be triggered, by passing
	// a base58 string of all 'z's for example.
	if binary_[0] > 0xFFFFFFFF {
		return false
	}

	// Convert each term to big endian for the final output
	for i := 0; i < bin32sz; i++ {
		binary.BigEndian.PutUint32(out[4*i:], uint32(binary_[i]))
	}

	// Make sure the encoded version has the same number of leading '1's
	// as the decoded version has leading 0s. The check doesn't read past
	// the end of encoded, because '\0' != '1', so it will return NULL.
	var leadingZeroCnt int
	for leadingZeroCnt = 0; leadingZeroCnt < 32; leadingZeroCnt++ {
		if out[leadingZeroCnt] != 0 {
			break
		}
		if encoded[leadingZeroCnt] != '1' {
			return false
		}
	}
	if leadingZeroCnt < len(encoded) && encoded[leadingZeroCnt] == '1' {
		return false
	}

	return true
}

func Decode64(out *[64]byte, encoded []byte) (ok bool) {
	// Check length
	if len(encoded) < 64 || len(encoded) > 88 {
		return false
	}

	// Validate string
	for _, c := range encoded {
		idx := int(c) - int(inverseLUTOffset)
		if idx > int(inverseLUTSentinel) {
			idx = int(inverseLUTSentinel)
		}
		if inverseLUT[idx] == invalidChar {
			return false
		}
	}

	// X = sum_i raw_base58[i] * 58^(RAW58_SZ-1-i)
	var rawBase58 [raw64sz]byte

	// Prepend enough 0s to make it exactly RAW58_SZ characters
	prepend0 := raw64sz - len(encoded)
	for j := 0; j < raw64sz; j++ {
		if j >= int(prepend0) {
			rawBase58[j] = inverseLUT[encoded[j-int(prepend0)]-inverseLUTOffset]
		}
	}

	// Convert to the intermediate format
	//   X = sum_i intermediate[i] * 58^(5*(INTERMEDIATE_SZ-1-i))
	var intermediate [inter64sz]uint64
	for i := 0; i < inter64sz; i++ {
		intermediate[i] = uint64(rawBase58[5*i+0])*11316496 +
			uint64(rawBase58[5*i+1])*195112 +
			uint64(rawBase58[5*i+2])*3364 +
			uint64(rawBase58[5*i+3])*58 +
			uint64(rawBase58[5*i+4])
	}

	// Using the table, convert to overcomplete base 2^32 (terms can be
	// larger than 2^32).  We need to be careful about overflow.
	//
	// For N==64, the largest anything in binary can get is binary[13]:
	// even if intermediate[i]==58^5-1 for all i, then binary[13] <
	// 2^63.998.  Hanging in there, just by a thread!
	var binary_ [bin64sz]uint64
	for j := 0; j < bin64sz; j++ {
		var acc uint64
		for i := 0; i < inter64sz; i++ {
			acc += uint64(intermediate[i]) * uint64(decTable64[i][j])
		}
		binary_[j] = acc
	}

	// Make sure each term is less than 2^32.
	//
	// For N==64, even if we add 2^32 to binary[13], it is still 2^63.998,
	// so this won't overflow.
	for i := bin64sz - 1; i > 0; i-- {
		binary_[i-1] += binary_[i] >> 32
		binary_[i] &= 0xFFFFFFFF
	}

	// If the largest term is 2^32 or bigger, it means N is larger than
	// what can fit in BYTE_CNT bytes.  This can be triggered, by passing
	// a base58 string of all 'z's for example.
	if binary_[0] > 0xFFFFFFFF {
		return false
	}

	// Convert each term to big endian for the final output
	for i := 0; i < bin64sz; i++ {
		binary.BigEndian.PutUint32(out[4*i:], uint32(binary_[i]))
	}

	// Make sure the encoded version has the same number of leading '1's
	// as the decoded version has leading 0s. The check doesn't read past
	// the end of encoded, because '\0' != '1', so it will return NULL.
	var leadingZeroCnt int
	for leadingZeroCnt = 0; leadingZeroCnt < 64; leadingZeroCnt++ {
		if out[leadingZeroCnt] != 0 {
			break
		}
		if encoded[leadingZeroCnt] != '1' {
			return false
		}
	}
	if leadingZeroCnt < len(encoded) && encoded[leadingZeroCnt] == '1' {
		return false
	}

	return true
}

func DecodeFromString(addr string) ([32]byte, error) {
	in := []byte(addr)
	var out [32]byte
	ok := Decode32(&out, in)
	if !ok {
		return out, fmt.Errorf("failed to decode base58 from string")
	}

	return out, nil
}

func MustDecodeFromString(addr string) [32]byte {
	out, err := DecodeFromString(addr)
	if err != nil {
		panic(err.Error())
	}
	return out
}

func Encode(buf []byte) string {
	switch len(buf) {
	case 32:
		var out [44]byte
		outLen := Encode32(&out, *(*[32]byte)(buf))
		return string(out[:outLen])
	case 64:
		var out [88]byte
		outLen := Encode64(&out, *(*[64]byte)(buf))
		return string(out[:outLen])
	default:
		panic("unsupported base58 length")
	}
}
