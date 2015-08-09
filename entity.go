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
