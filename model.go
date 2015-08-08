package appx

import "appengine/datastore"

type Model struct {
	key       *datastore.Key
	parentKey *datastore.Key
}

func (model *Model) Key() *datastore.Key {
	return model.key
}

func (model *Model) SetKey(key *datastore.Key) {
	model.key = key
	model.parentKey = key.Parent()
}

func (model *Model) ParentKey() *datastore.Key {
	return model.parentKey
}

func (model *Model) SetParentKey(key *datastore.Key) {
	model.parentKey = key
}

func (model *Model) HasKey() bool {
	return model.key != nil
}

func (model *Model) EncodedKey() string {
	return model.Key().Encode()
}

func (model *Model) SetEncodedKey(id string) error {
	key, err := datastore.DecodeKey(id)
	if err != nil {
		return err
	}
	model.SetKey(key)
	model.SetParentKey(key.Parent())
	return nil
}
