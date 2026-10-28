//go:build amd64 && !purego

package cjson

func ParseUuid(str string) (res [2]uint64, err error) {
	switch len(str) {
	case 0:
		return res, nil
	case 32, 36:
		hi, lo, ok := parseUuidFastAsm(str)
		if ok {
			return [2]uint64{hi, lo}, nil
		}
	}
	return parseUuidSlow(str)
}

//go:noescape
func parseUuidFastAsm(str string) (hi uint64, lo uint64, ok bool)
