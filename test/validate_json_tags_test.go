package reindexer

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/restream/reindexer/v5"
	"github.com/restream/reindexer/v5/cjson"
)

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

const testValidateJsonTagsNs = "validate_json_tags"

var (
	errMessageFormat = "Struct is invalid. JSON tag '%s' duplicate at field '%s' (type: %s)"

	rootLevelErrMessage                       = fmt.Sprintf(errMessageFormat, "title", "Desc", "string")
	internalLevelErrMessage                   = fmt.Sprintf(errMessageFormat, "name", "Title.Surname", "string")
	deepInternalLevelErrMessage               = fmt.Sprintf(errMessageFormat, "name", "Deep.Fail.Title.Surname", "string")
	deepInternalLevelWithOmitSymbolErrMessage = fmt.Sprintf(errMessageFormat, "name", "DeepInternalLevelFailType.Deep.Fail.Title.Surname", "string")
	embeddedInternalLevelErrMessage           = fmt.Sprintf(errMessageFormat, "Price", "EmbeddedFail.Price", "int")
)

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

func TestReturnRegisterNamespaceError(t *testing.T) {
	const ns = testValidateJsonTagsNs

	NsOptions := reindexer.DefaultNamespaceOptions()
	assert.Equal(t, fmt.Errorf(rootLevelErrMessage), OpenNamespaceWrapper(ns, NsOptions, RootLevelFailType{}))
	assert.Equal(t, fmt.Errorf(internalLevelErrMessage), OpenNamespaceWrapper(ns, NsOptions, InternalLevelFailType{}))
	assert.Equal(t, fmt.Errorf(deepInternalLevelErrMessage), OpenNamespaceWrapper(ns, NsOptions, DeepInternalLevelFailType{}))
	require.NoError(t, OpenNamespaceWrapper(ns, NsOptions, DBItemType{}))
	DB.CloseNamespace(ns)
	require.NoError(t, OpenNamespaceWrapper(ns, NsOptions, ServiceType{}))
	DB.CloseNamespace(ns)
	require.NoError(t, OpenNamespaceWrapper(ns, NsOptions, ElementType{}))
	DB.CloseNamespace(ns)
	assert.Equal(t, fmt.Errorf(deepInternalLevelWithOmitSymbolErrMessage), OpenNamespaceWrapper(ns, NsOptions, DBItemFailType{}))
	assert.Equal(t, fmt.Errorf(embeddedInternalLevelErrMessage), OpenNamespaceWrapper(ns, NsOptions, EmbeddedFailType{}))
	require.NoError(t, OpenNamespaceWrapper(ns, NsOptions, EmbeddedSuccessType{}))
	DB.CloseNamespace(ns)
	require.NoError(t, OpenNamespaceWrapper(ns, NsOptions, RecursiveType{}))
	DB.CloseNamespace(ns)
	require.NoError(t, OpenNamespaceWrapper(ns, NsOptions, &RecursiveTypePtr{}))
	DB.CloseNamespace(ns)
}
