package appx

import (
	"appengine/datastore"
	"errors"
)

var (
	ErrMissingEntityKind   = errors.New("Entity Kind cannot be empty")
	ErrIncompleteKey       = errors.New("Entity has incomplete key")
	ErrMissingParentKey    = errors.New("Entity parent key is missing")
	ErrIncompleteParentKey = errors.New("Entity parent key cannot be incomplete")
	ErrInvalidEntityType   = errors.New("Invalid entity type. Make sure your model implements appx.Entity")
	ErrInvalidSliceType   = errors.New("Invalid slice type. Make sure you pass a pointer to a slice of appx.Entity")
)

type Entity interface {
	KeySpec() *KeySpec
	HasKey() bool
	Key() *datastore.Key
	SetKey(*datastore.Key)
	ParentKey() *datastore.Key
	SetParentKey(*datastore.Key)
}

type Cacheable interface {
	CacheID() string
}

type Queryable interface {
	Query() *datastore.Query
}

type CachedEntity struct {
	Key       *datastore.Key
	ParentKey *datastore.Key
	Entity    Entity
}

type Iterator interface {
	LoadNext(interface{}) error
	HasNext() bool
	Cursor() string
}
