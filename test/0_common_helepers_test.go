package reindexer

import (
	"fmt"
	"testing"

	"github.com/restream/reindexer/v5"
)

func assertErrorMessage(t *testing.T, actual error, expected error) {
	if fmt.Sprintf("%v", actual) != fmt.Sprintf("%v", expected) {
		t.Fatalf("Error actual = %v, and Expected = %v.", actual, expected)
	}
}

func OpenNamespaceWrapper(ns string, opts *reindexer.NamespaceOptions, s interface{}) (err error) {
	defer func() {
		if ierr := recover(); ierr != nil {
			err = ierr.(error)
			return
		}
	}()
	return DB.OpenNamespace(ns, opts, s)
}
