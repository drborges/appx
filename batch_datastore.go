package appx

import (
	"appengine/datastore"
	"github.com/drborges/riversv2/rx"
)

type BatchDatastore struct {
	Size  int
	Keys  []*datastore.Key
	Items []Entity
}

func (batch *BatchDatastore) Full() bool {
	return len(batch.Items) == batch.Size
}

func (batch *BatchDatastore) Empty() bool {
	return len(batch.Items) == 0
}

func (batch *BatchDatastore) Add(data rx.T) {
	entity := data.(Entity)
	batch.Keys = append(batch.Keys, entity.Key())
	batch.Items = append(batch.Items, entity)
}

func (batch *BatchDatastore) Commit(out rx.OutStream) {
	out <- &BatchDatastore{
		Size:  batch.Size,
		Keys:  batch.Keys,
		Items: batch.Items,
	}

	batch.Items = []Entity{}
	batch.Keys = []*datastore.Key{}
}
