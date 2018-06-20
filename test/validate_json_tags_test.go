package reindexer

import (
	"fmt"
	"testing"
	"time"

	"github.com/restream/reindexer"
	"github.com/restream/reindexer/cjson"
)

var nsName = "validate_json_tags"
var errMessageFormat = "Struct is invalid. JSON tag '%s' duplicate at field '%s' (type: %s)"

type titleType struct {
	Name string `json:"name"`
}

type titleInternalFailType struct {
	Name    string `json:"name"`
	Surname string `json:"name"`
}

type RootLevelFailType struct {
	ID    int        `reindex:"id,,pk"`
	Title *titleType `json:"title"`
	Desc  string     `json:"title"`
}

type InternalLevelFailType struct {
	ID    int                    `reindex:"id,,pk"`
	Title *titleInternalFailType `json:"title"`
	Desc  string                 `json:"desc"`
}

type DeepInternalLevelFailType struct {
	ID   int `reindex:"id,,pk"`
	Deep struct {
		Fail *InternalLevelFailType
	} `json:"deep"`
	Desc string `json:"desc"`
}

type DBItemType struct {
	UpdatedAt *time.Time `json:"-" db:"updated_at"`
	DeletedAt *time.Time `json:"-" db:"deleted_at"`
	CreatedAt *time.Time `json:"-" db:"created_at"`
}

type DBItemFailType struct {
	DBItemType
	DeepInternalLevelFailType
}

type ServiceType struct {
	titleType
	DBItemType
	item  DBItemType
	Items []*DBItemType
}

type ElementType struct {
	DBItemType
	Item  *DBItemType
	Items *[]DBItemType
}

type EmbeddedSuccess struct {
	Number int
	Price  int
}

type EmbeddedFail struct {
	Number int `json:"Price"`
	Price  int `json:"Price"`
}

type EmbeddedSuccessType struct {
	EmbeddedSuccess
	Price int
}

type EmbeddedFailType struct {
	EmbeddedFail
	Number int
}

var (
	rootLevelErrMessage                       = fmt.Sprintf(errMessageFormat, "title", "Desc", "string")
	internalLevelErrMessage                   = fmt.Sprintf(errMessageFormat, "name", "Title.Surname", "string")
	deepInternalLevelErrMessage               = fmt.Sprintf(errMessageFormat, "name", "Deep.Fail.Title.Surname", "string")
	deepInternalLevelWithOmitSymbolErrMessage = fmt.Sprintf(errMessageFormat, "name", "DeepInternalLevelFailType.Deep.Fail.Title.Surname", "string")
	embeddedInternalLevelErrMessage           = fmt.Sprintf(errMessageFormat, "Price", "EmbeddedFail.Price", "int")
)

func assertErrorMessage(t *testing.T, actual error, expected error) {
	if fmt.Sprintf("%v", actual) != fmt.Sprintf("%v", expected) {
		t.Fatalf("Error actual = %v, and Expected = %v.", actual, expected)
	}
}

func TestReturnEncoderValidateError(t *testing.T) {
	enc := cjson.Encoder{}

	assertErrorMessage(t, enc.Validate(RootLevelFailType{}), fmt.Errorf(rootLevelErrMessage))
	assertErrorMessage(t, enc.Validate(InternalLevelFailType{}), fmt.Errorf(internalLevelErrMessage))
	assertErrorMessage(t, enc.Validate(DeepInternalLevelFailType{}), fmt.Errorf(deepInternalLevelErrMessage))
	assertErrorMessage(t, enc.Validate(DBItemType{}), nil)
	assertErrorMessage(t, enc.Validate(ServiceType{}), nil)
	assertErrorMessage(t, enc.Validate(ElementType{}), nil)
	assertErrorMessage(t, enc.Validate(DBItemFailType{}), fmt.Errorf(deepInternalLevelWithOmitSymbolErrMessage))
	assertErrorMessage(t, enc.Validate(EmbeddedSuccessType{}), nil)
	assertErrorMessage(t, enc.Validate(EmbeddedFailType{}), fmt.Errorf(embeddedInternalLevelErrMessage))
}

func TestReturnOpenNamespaceError(t *testing.T) {
	NsOptions := reindexer.DefaultNamespaceOptions()
	assertErrorMessage(t, DB.OpenNamespace(nsName, NsOptions, RootLevelFailType{}), fmt.Errorf(rootLevelErrMessage))
	assertErrorMessage(t, DB.OpenNamespace(nsName, NsOptions, InternalLevelFailType{}), fmt.Errorf(internalLevelErrMessage))
	assertErrorMessage(t, DB.OpenNamespace(nsName, NsOptions, DeepInternalLevelFailType{}), fmt.Errorf(deepInternalLevelErrMessage))
	assertErrorMessage(t, DB.OpenNamespace(nsName, NsOptions, DBItemType{}), nil)
	DB.CloseNamespace(nsName)
	assertErrorMessage(t, DB.OpenNamespace(nsName, NsOptions, ServiceType{}), nil)
	DB.CloseNamespace(nsName)
	assertErrorMessage(t, DB.OpenNamespace(nsName, NsOptions, ElementType{}), nil)
	DB.CloseNamespace(nsName)
	assertErrorMessage(t, DB.OpenNamespace(nsName, NsOptions, DBItemFailType{}), fmt.Errorf(deepInternalLevelWithOmitSymbolErrMessage))
	assertErrorMessage(t, DB.OpenNamespace(nsName, NsOptions, EmbeddedFailType{}), fmt.Errorf(embeddedInternalLevelErrMessage))
	assertErrorMessage(t, DB.OpenNamespace(nsName, NsOptions, EmbeddedSuccessType{}), nil)
	DB.CloseNamespace(nsName)
}
