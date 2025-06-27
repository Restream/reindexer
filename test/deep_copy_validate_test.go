package reindexer

import (
	"testing"

	"github.com/restream/reindexer/v5"
)

type DeepCopy interface {
	DeepCopy() interface{}
}

type PurchaseOption struct {
	CurID          int    `json:"id" reindex:"id,,pk"`
	BonusPriceID   int64  `json:"bonus_price_id"`
	PackageID      int64  `json:"package_id"`
	Amount         int    `json:"amount"`
	Currency       string `json:"currency"`
	PriceDesc      string `json:"price_desc"`
	Icon           string `json:"icon"`
	UsageModel     string `json:"usage_model"`
	Period         int    `json:"period"`
	ServiceConsist string `json:"service_consist,omitempty"`
	ExternalID     string `json:"external_id"`
	PortalID       string `json:"portal_id"`
	AssetType      int    `json:"asset_type"`
	AppleID        string `json:"apple_id"`
	AndroidID      string `json:"android_id"`
}

type BrokenDeepCopyType struct {
	ID    int    `reindex:"id,,pk`
	Name  string `reindex:"name"`
	Value int64  `json:"value"`
}

type SimpleDeepCopyType struct {
	ID    int    `reindex:"id,,pk`
	Name  string `reindex:"name"`
	Value int64  `json:"value"`
}

type NestedDeepCopyType struct {
	po *PurchaseOption
	SimpleDeepCopyType
}

const testDeepCopyNs = "test_deep_copy_type_equality"

func (po *PurchaseOption) DeepCopy() interface{} {
	return &PurchaseOption{
		CurID:          po.CurID,
		BonusPriceID:   po.BonusPriceID,
		PackageID:      po.PackageID,
		Amount:         po.Amount,
		Currency:       po.Currency,
		PriceDesc:      po.PriceDesc,
		Icon:           po.Icon,
		UsageModel:     po.UsageModel,
		Period:         po.Period,
		ServiceConsist: po.ServiceConsist,
		ExternalID:     po.ExternalID,
		PortalID:       po.PortalID,
		AssetType:      po.AssetType,
		AppleID:        po.AppleID,
		AndroidID:      po.AndroidID,
	}

}

func gencopyPoFieldCopy(in *PurchaseOption) *PurchaseOption {
	if in == nil {
		return nil
	}
	return in.DeepCopy().(*PurchaseOption)
}

func (bt *BrokenDeepCopyType) DeepCopy() interface{} {
	return &PurchaseOption{}
}

func (st *SimpleDeepCopyType) DeepCopy() interface{} {
	return &SimpleDeepCopyType{
		ID:    st.ID,
		Name:  st.Name,
		Value: st.Value,
	}
}

func (n *NestedDeepCopyType) DeepCopy() interface{} {
	return &NestedDeepCopyType{
		po:                 gencopyPoFieldCopy(n.po),
		SimpleDeepCopyType: n.SimpleDeepCopyType,
	}
}

func TestDeepCopyEquality(t *testing.T) {
	nsOpts := reindexer.DefaultNamespaceOptions()
	DB.DropNamespace(testDeepCopyNs)

	assertErrorMessage(t, OpenNamespaceWrapper(testDeepCopyNs, nsOpts, PurchaseOption{}), nil)
	assertErrorMessage(t, DB.DropNamespace(testDeepCopyNs), nil)
	assertErrorMessage(t, OpenNamespaceWrapper(testDeepCopyNs, nsOpts, BrokenDeepCopyType{}), reindexer.ErrDeepCopyType)

	assertErrorMessage(t, OpenNamespaceWrapper(testDeepCopyNs, nsOpts, NestedDeepCopyType{}), nil)
	assertErrorMessage(t, DB.DropNamespace(testDeepCopyNs), nil)

}
