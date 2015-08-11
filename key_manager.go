package appx

import (
	"appengine"
	"appengine/datastore"
)

type KeyManager struct {
	context appengine.Context
}

func NewKeyManager(context appengine.Context) *KeyManager {
	return &KeyManager{context}
}

func (resolver *KeyManager) Resolve(e Entity) error {
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

func (resolver *KeyManager) Assign(e Entity) error {
	spec := e.KeySpec()

	if err := spec.Validate(e); err != nil && err != ErrIncompleteKey {
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
