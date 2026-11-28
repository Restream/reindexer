package reindexer

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/restream/reindexer/v5"
)

func assertErrorMessage(t *testing.T, actual error, expected error) {
	if fmt.Sprintf("%v", actual) != fmt.Sprintf("%v", expected) {
		t.Fatalf("Error actual = %v, and Expected = %v.", actual, expected)
	}
}

func OpenNamespaceWrapper(ns string, opts *reindexer.NamespaceOptions, s any) (err error) {
	defer func() {
		if ierr := recover(); ierr != nil {
			err = ierr.(error)
			return
		}
	}()
	return DB.OpenNamespace(ns, opts, s)
}

func GetRandomElementFromSlice(slice any) any {
	v := reflect.ValueOf(slice)
	switch v.Kind() {
	case reflect.Slice, reflect.Array:
		if v.Len() == 0 {
			return nil
		}
		return v.Index(rand.Intn(v.Len())).Interface()
	default:
		return slice
	}
}
