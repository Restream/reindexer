package reindexer

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v3"
	"github.com/restream/reindexer/v3/cjson"
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

type RecursiveType struct {
	ID            int             `json:"id" reindex:"id,,pk"`
	Recursive     []RecursiveType `json:"recursive"`
	RecursiveWrap RecursiveWrap
}

type RecursiveWrap struct {
	Recursive []RecursiveType
}

type RecursiveTypePtr struct {
	ID            int                 `json:"id" reindex:"id,,pk"`
	Recursive     []*RecursiveTypePtr `json:"recursive"`
	RecursiveWrap *RecursiveWrapPtr
}

type RecursiveWrapPtr struct {
	Recursive []*RecursiveTypePtr
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
	enc := cjson.Validator{}

	assert.Equal(t, fmt.Errorf(rootLevelErrMessage), enc.Validate(RootLevelFailType{}))
	assert.Equal(t, fmt.Errorf(internalLevelErrMessage), enc.Validate(InternalLevelFailType{}))
	assert.Equal(t, fmt.Errorf(deepInternalLevelErrMessage), enc.Validate(DeepInternalLevelFailType{}))
	require.NoError(t, enc.Validate(DBItemType{}))
	require.NoError(t, enc.Validate(ServiceType{}))
	require.NoError(t, enc.Validate(ElementType{}))
	assert.Equal(t, fmt.Errorf(deepInternalLevelWithOmitSymbolErrMessage), enc.Validate(DBItemFailType{}))
	require.NoError(t, enc.Validate(EmbeddedSuccessType{}))
	assert.Equal(t, fmt.Errorf(embeddedInternalLevelErrMessage), enc.Validate(EmbeddedFailType{}))
	require.NoError(t, enc.Validate(RecursiveType{}))
	require.NoError(t, enc.Validate(&RecursiveTypePtr{}))
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

func TestReturnRegisterNamespaceError(t *testing.T) {
	NsOptions := reindexer.DefaultNamespaceOptions()
	assert.Equal(t, fmt.Errorf(rootLevelErrMessage), OpenNamespaceWrapper(nsName, NsOptions, RootLevelFailType{}))
	assert.Equal(t, fmt.Errorf(internalLevelErrMessage), OpenNamespaceWrapper(nsName, NsOptions, InternalLevelFailType{}))
	assert.Equal(t, fmt.Errorf(deepInternalLevelErrMessage), OpenNamespaceWrapper(nsName, NsOptions, DeepInternalLevelFailType{}))
	require.NoError(t, OpenNamespaceWrapper(nsName, NsOptions, DBItemType{}))
	DB.CloseNamespace(nsName)
	require.NoError(t, OpenNamespaceWrapper(nsName, NsOptions, ServiceType{}))
	DB.CloseNamespace(nsName)
	require.NoError(t, OpenNamespaceWrapper(nsName, NsOptions, ElementType{}))
	DB.CloseNamespace(nsName)
	assert.Equal(t, fmt.Errorf(deepInternalLevelWithOmitSymbolErrMessage), OpenNamespaceWrapper(nsName, NsOptions, DBItemFailType{}))
	assert.Equal(t, fmt.Errorf(embeddedInternalLevelErrMessage), OpenNamespaceWrapper(nsName, NsOptions, EmbeddedFailType{}))
	require.NoError(t, OpenNamespaceWrapper(nsName, NsOptions, EmbeddedSuccessType{}))
	DB.CloseNamespace(nsName)
	require.NoError(t, OpenNamespaceWrapper(nsName, NsOptions, RecursiveType{}))
	DB.CloseNamespace(nsName)
	require.NoError(t, OpenNamespaceWrapper(nsName, NsOptions, &RecursiveTypePtr{}))
	DB.CloseNamespace(nsName)
}
