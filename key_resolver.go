package appx

import (
	"appengine"
	"appengine/datastore"
)

type KeyResolver struct {
	context appengine.Context
}

func NewKeyResolver(context appengine.Context) *KeyResolver {
	return &KeyResolver{context}
}

func (resolver *KeyResolver) Resolve(e Entity) error {
	spec := e.KeySpec()

	if err := spec.Validate(e); err != nil {
		return err
	}

	e.SetKey(datastore.NewKey(
		resolver.context,
		spec.Kind,
		spec.StringID,
		spec.IntID,
		e.ParentKey()))

	return nil
}
