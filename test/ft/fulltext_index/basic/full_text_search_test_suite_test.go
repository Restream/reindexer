package basic

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestFulltextSearchTest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Full text search (fulltext index) basic test suite")
}
