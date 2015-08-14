package appx_test

import "github.com/drborges/appx"

type Tag struct {
	appx.Model
	Name  string
	Owner string
}

// KeyMetadata in conjunction with appx.Model implement
// appx.Entity interface making Tag compatible with
// appx.Datastore
//
// A tag key is defined to use its name as the StringID
// component in the datastore key
func (tag *Tag) KeySpec() *appx.KeySpec {
	return &appx.KeySpec{
		Kind:     "Tags",
		StringID: tag.Name,
	}
}
