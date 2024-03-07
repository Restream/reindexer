package cjson

import "fmt"

type tagsMatcher struct {
	Tags  []string
	Names map[string]int
}

func (tm *tagsMatcher) Read(ser *Serializer, skip bool) {

	tagsCount := int(ser.GetVarUInt())
	if !skip {
		tm.Tags = make([]string, tagsCount, tagsCount)
		tm.Names = make(map[string]int)

		for i := 0; i < tagsCount; i++ {
			tm.Tags[i] = ser.GetVString()
			tm.Names[tm.Tags[i]] = i
		}
	} else {
		for i := 0; i < tagsCount; i++ {
			ser.GetVString()
		}
	}
}

func (tm *tagsMatcher) WriteUpdated(ser *Serializer) {
	ser.PutVarUInt(uint64(len(tm.Tags)))
	for i := 0; i < len(tm.Tags); i++ {
		ser.PutVString(tm.Tags[i])
	}
}

func (tm *tagsMatcher) tag2name(tag int16) string {
	tag = tag & ((1 << 12) - 1)
	if tag == 0 {
		return ""
	}
	if int(tag)-1 >= len(tm.Tags) {
		panic(fmt.Errorf("Internal error - unknown tag %d\nKnown tags: %v", tag, tm.Names))
	}

	return tm.Tags[tag-1]
}

func (tm *tagsMatcher) name2tag(name string, canAdd bool) int {

	tag, ok := tm.Names[name]

	if !ok {
		if canAdd {
			if tm.Names == nil {
				tm.Names = make(map[string]int)
			}
			tag = len(tm.Tags)
			tm.Names[name] = tag
			tm.Tags = append(tm.Tags, name)
		} else {
			return 0
		}
	}
	return tag + 1
}
