//go:build !amd64 || purego

package cjson

const invalidUuidPair = uint16(0xffff)

var uuidHexPairToByte = func() [1 << 16]uint16 {
	var table [1 << 16]uint16
	for i := range table {
		table[i] = invalidUuidPair
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
			table[uint16(hi)<<8|uint16(lo)] = uint16(hiVal<<4 | loVal)
		}
	}
	return table
}()

func ParseUuid(str string) (res [2]uint64, err error) {
	if res, ok := parseUuidFastGo(str); ok {
		return res, nil
	}
	return parseUuidSlow(str)
}

func uuidHexPair(str string, pos int) uint16 {
	return uuidHexPairToByte[uint16(str[pos])<<8|uint16(str[pos+1])]
}

func parseUuidFastGo(str string) (res [2]uint64, ok bool) {
	var p0, p1, p2, p3, p4, p5, p6, p7 uint16
	var p8, p9, p10, p11, p12, p13, p14, p15 uint16

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

	if p0|p1|p2|p3|p4|p5|p6|p7|p8|p9|p10|p11|p12|p13|p14|p15 > 0xff {
		return res, false
	}
	res[0] = uint64(p0)<<56 | uint64(p1)<<48 | uint64(p2)<<40 | uint64(p3)<<32 |
		uint64(p4)<<24 | uint64(p5)<<16 | uint64(p6)<<8 | uint64(p7)
	res[1] = uint64(p8)<<56 | uint64(p9)<<48 | uint64(p10)<<40 | uint64(p11)<<32 |
		uint64(p12)<<24 | uint64(p13)<<16 | uint64(p14)<<8 | uint64(p15)
	if (res[0] != 0 || res[1] != 0) && (res[1]>>63) == 0 {
		return res, false
	}
	return res, true
}
