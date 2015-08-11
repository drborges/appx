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
	size     int
}

func (batch *DatastoreBatch) Full() bool {
	return len(batch.entities) == batch.size
}

func (batch *DatastoreBatch) Empty() bool {
	return len(batch.entities) == 0
}

func (batch *DatastoreBatch) Add(data rx.T) {
	if entity, ok := data.(Entity); ok {
		batch.keys = append(batch.keys, entity.Key())
		batch.entities = append(batch.entities, entity)
	}
}

type DatastoreBatchLoader struct {
	DatastoreBatch
}

func NewDatastoreBatchLoaderWithSize(context appengine.Context, size int) *DatastoreBatchLoader {
	batch := &DatastoreBatchLoader{}
	batch.context = context
	batch.size = size
	return batch
}

func (batch *DatastoreBatchLoader) Commit(out rx.OutStream) {
	if err := datastore.GetMulti(batch.context, batch.keys, batch.entities); err != nil {
		panic(err)
	}
	batch.entities = []Entity{}
	batch.keys = []*datastore.Key{}
}

type DatastoreBatchSaver struct {
	DatastoreBatch
}

func NewDatastoreBatchSaverWithSize(context appengine.Context, size int) *DatastoreBatchSaver {
	batch := &DatastoreBatchSaver{}
	batch.context = context
	batch.size = size
	return batch
}

func (batch *DatastoreBatchSaver) Commit(out rx.OutStream) {
	keys, err := datastore.PutMulti(batch.context, batch.keys, batch.entities)
	if err != nil {
		panic(err)
	}

	// Set refreshed keys back to the entities
	// For new entities with incomplete keys, the actual
	// key is the one returned by PutMulti
	for i, key := range keys {
		batch.entities[i].SetKey(key)
	}

	batch.entities = []Entity{}
	batch.keys = []*datastore.Key{}
}
