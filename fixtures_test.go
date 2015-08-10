package appx_test

import (
	"github.com/drborges/appxv2"
	"appengine/datastore"
)

type Entity struct {
	appx.Model
	keySpec *appx.KeySpec
}

func (entity Entity) KeySpec() *appx.KeySpec {
	return entity.keySpec
}

type User struct {
	appx.Model
	Name    string
	Email   string
	keySpec *appx.KeySpec
}

func (user *User) KeySpec() *appx.KeySpec {
	return user.keySpec
}

func (user *User) CacheID() string {
	return user.keySpec.StringID
}

func (user *User) Query() *datastore.Query {
	return datastore.NewQuery(user.keySpec.Kind).Filter("Email=", user.Email).Limit(1)
}
