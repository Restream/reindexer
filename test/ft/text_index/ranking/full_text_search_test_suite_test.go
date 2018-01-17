package ranking_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestFulltextSearchTest(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Full text search (text index) ranking test suite")
}
