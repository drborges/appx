package appx

import (
	"appengine"
	"github.com/drborges/riversv2"
)

type Datastore struct {
	context appengine.Context
}

func NewDatastore(context appengine.Context) *Datastore {
	return &Datastore{context}
}

func (datastore *Datastore) Load(e Entity) error {
	return datastore.LoadAll(e)
}

func (datastore *Datastore) LoadAll(entities ...Entity) error {
	context := rivers.NewContext()
	transformer := NewTransformer(context)
	rivers.NewWith(context).FromSlice(entities).
		Apply(transformer.ResolveEntityKey(datastore.context)).
		Apply(transformer.LoadEntitiesFromCacheInBatch(datastore.context)).
		Apply(transformer.LookupEntitiesFromDatastoreInBatch(datastore.context)).
		Apply(transformer.QueryEntityFromDatastore(datastore.context)).
		Drain()

	return context.Err()
}
