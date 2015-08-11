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

func (datastore *Datastore) Load(entity Entity) error {
	return datastore.LoadAll(entity)
}

func (datastore *Datastore) LoadAll(entities ...Entity) error {
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

func (datastore *Datastore) Save(entity Entity) error {
	return datastore.SaveAll(entity)
}

func (datastore *Datastore) SaveAll(entities ...Entity) error {
	context := rivers.NewContext()
	transformer := NewTransformer(context)
	rivers.NewWith(context).FromSlice(entities).
		Apply(transformer.ResolveEntityKey(datastore.context)).
		Apply(transformer.UpdateEntitiesInDatastore(datastore.context)).
		Apply(transformer.UpdateEntitiesInCache(datastore.context)).
		Drain()

	return context.Err()
}