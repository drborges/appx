package appx

import (
	"appengine"
	"appengine/datastore"
	"github.com/drborges/riversv2"
)

type Datastore struct {
	context appengine.Context
}

func NewDatastore(context appengine.Context) *Datastore {
	return &Datastore{context}
}

func (datastore *Datastore) Load(entities ...Entity) error {
	context := rivers.NewContext()
	transformer := NewTransformer(context)
	rivers.NewWith(context).FromSlice(entities).
		Apply(transformer.ResolveEntityKey(datastore.context)).
		Apply(transformer.LoadEntitiesFromCache(datastore.context)).
		Apply(transformer.LookupEntitiesFromDatastore(datastore.context)).
		Apply(transformer.QueryEntityFromDatastore(datastore.context)).
		Drain()

	return context.Err()
}

func (datastore *Datastore) Save(entities ...Entity) error {
	context := rivers.NewContext()
	transformer := NewTransformer(context)
	rivers.NewWith(context).FromSlice(entities).
		Apply(transformer.ResolveEntityKey(datastore.context)).
		Apply(transformer.UpdateEntitiesInDatastore(datastore.context)).
		Apply(transformer.UpdateEntitiesInCache(datastore.context)).
		Drain()

	return context.Err()
}

func (datastore *Datastore) Delete(entities ...Entity) error {
	context := rivers.NewContext()
	transformer := NewTransformer(context)
	rivers.NewWith(context).FromSlice(entities).
		Apply(transformer.ResolveEntityKey(datastore.context)).
		Apply(transformer.DeleteEntitiesFromCache(datastore.context)).
		Apply(transformer.DeleteEntitiesFromDatastore(datastore.context)).
		Drain()

	return context.Err()
}

func (datastore *Datastore) Query(q *datastore.Query) *runner {
	return &runner{datastore.context, q}
}
