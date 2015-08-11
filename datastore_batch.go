package appx

import (
	"appengine"
	"appengine/datastore"
	"github.com/drborges/riversv2/rx"
)

type DatastoreBatch struct {
	context  appengine.Context
	keys     []*datastore.Key
	entities []Entity
}

func NewDatastoreBatch(context appengine.Context) *DatastoreBatch {
	return &DatastoreBatch{
		context: context,
	}
}

func (batch *DatastoreBatch) Full() bool {
	return len(batch.entities) == 1000
}

func (batch *DatastoreBatch) Empty() bool {
	return len(batch.entities) == 0
}

func (batch *DatastoreBatch) Commit(out rx.OutStream) {
	if err := datastore.GetMulti(batch.context, batch.keys, batch.entities); err != nil {
		panic(err)
	}
}

func (batch *DatastoreBatch) Add(data rx.T) {
	if entity, ok := data.(Entity); ok {
		batch.keys = append(batch.keys, entity.Key())
		batch.entities = append(batch.entities, entity)
	}
}
