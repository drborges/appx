package appx

import (
	"appengine/datastore"
	"github.com/drborges/rivers/stream"
)

type DatastoreBatch struct {
	Size  int
	Keys  []*datastore.Key
	Items []Entity
}

func (batch *DatastoreBatch) Full() bool {
	return len(batch.Items) == batch.Size
}

func (batch *DatastoreBatch) Empty() bool {
	return len(batch.Items) == 0
}

func (batch *DatastoreBatch) Add(data stream.T) {
	entity := data.(Entity)
	batch.Keys = append(batch.Keys, entity.Key())
	batch.Items = append(batch.Items, entity)
}

func (batch *DatastoreBatch) Commit(out stream.Writable) {
	out <- &DatastoreBatch{
		Size:  batch.Size,
		Keys:  batch.Keys,
		Items: batch.Items,
	}

	batch.Items = []Entity{}
	batch.Keys = []*datastore.Key{}
}
