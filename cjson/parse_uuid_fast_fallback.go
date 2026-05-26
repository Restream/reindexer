//go:build !amd64 || purego

package cjson

func ParseUuid(str string) (res [2]uint64, err error) {
	if res, ok := parseUuidFastGo(str); ok {
		return res, nil
	}
	return parseUuidSlow(str)
}

// 0xff is also a valid decoded byte, so UUIDs containing ff fall through to parseUuidSlow.
const invalidUuidByte = byte(0xff)

var uuidHexPairToByte = func() [1 << 16]byte {
	var table [1 << 16]byte
	for i := range table {
		table[i] = invalidUuidByte
	}
	for hi := range 256 {
		hiVal := hexCharToUint[hi]
		if hiVal > 15 {
			continue
		}
		for lo := range 256 {
			loVal := hexCharToUint[lo]
			if loVal > 15 {
				continue
			}
			table[hi<<8|lo] = byte(hiVal<<4 | loVal)
		}
	}
	return table
}()

func hasFFByte(v uint64) bool {
	v = ^v
	return ((v - 0x0101010101010101) &^ v & 0x8080808080808080) != 0
}

func uuidHexPair(str string, pos int) byte {
	return uuidHexPairToByte[uint16(str[pos])<<8|uint16(str[pos+1])]
}

func parseUuidFastGo(str string) (res [2]uint64, ok bool) {
	var p0, p1, p2, p3, p4, p5, p6, p7 byte
	var p8, p9, p10, p11, p12, p13, p14, p15 byte

	switch len(str) {
	case 0:
		return res, true
	case 32:
		p0, p1, p2, p3 = uuidHexPair(str, 0), uuidHexPair(str, 2), uuidHexPair(str, 4), uuidHexPair(str, 6)
		p4, p5, p6, p7 = uuidHexPair(str, 8), uuidHexPair(str, 10), uuidHexPair(str, 12), uuidHexPair(str, 14)
		p8, p9, p10, p11 = uuidHexPair(str, 16), uuidHexPair(str, 18), uuidHexPair(str, 20), uuidHexPair(str, 22)
		p12, p13, p14, p15 = uuidHexPair(str, 24), uuidHexPair(str, 26), uuidHexPair(str, 28), uuidHexPair(str, 30)
	case 36:
		if str[8] != '-' || str[13] != '-' || str[18] != '-' || str[23] != '-' {
			return res, false
		}
		p0, p1, p2, p3 = uuidHexPair(str, 0), uuidHexPair(str, 2), uuidHexPair(str, 4), uuidHexPair(str, 6)
		p4, p5, p6, p7 = uuidHexPair(str, 9), uuidHexPair(str, 11), uuidHexPair(str, 14), uuidHexPair(str, 16)
		p8, p9, p10, p11 = uuidHexPair(str, 19), uuidHexPair(str, 21), uuidHexPair(str, 24), uuidHexPair(str, 26)
		p12, p13, p14, p15 = uuidHexPair(str, 28), uuidHexPair(str, 30), uuidHexPair(str, 32), uuidHexPair(str, 34)
	default:
		return res, false
	}

	res[0] = uint64(p0)<<56 | uint64(p1)<<48 | uint64(p2)<<40 | uint64(p3)<<32 |
		uint64(p4)<<24 | uint64(p5)<<16 | uint64(p6)<<8 | uint64(p7)
	res[1] = uint64(p8)<<56 | uint64(p9)<<48 | uint64(p10)<<40 | uint64(p11)<<32 |
		uint64(p12)<<24 | uint64(p13)<<16 | uint64(p14)<<8 | uint64(p15)
	if hasFFByte(res[0]) || hasFFByte(res[1]) {
		return res, false
	}
	if (res[0] != 0 || res[1] != 0) && (res[1]>>63) == 0 {
		return res, false
	}
	return res, true
}
