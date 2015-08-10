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
	context := rivers.NewContext()
	transformer := NewTransformer(context)
	rivers.NewWith(context).FromData(e).
		Apply(transformer.ResolveEntityKey(datastore.context)).
		Apply(transformer.LoadEntityFromCache(datastore.context)).
		Apply(transformer.LookupEntityFromDatastore(datastore.context)).
		Apply(transformer.QueryEntityFromDatastore(datastore.context)).
		Drain()

	return context.Err()
}