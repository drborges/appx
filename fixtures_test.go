package appx_test

import (
	"appengine/datastore"
	"github.com/drborges/appxv2"
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
	SSN     string
	keySpec *appx.KeySpec
}

func NewUserWithParent(user User) *User {
	return &User{
		Name:  user.Name,
		Email: user.Email,
		SSN: user.SSN,
		keySpec: &appx.KeySpec{
			Kind:      "Users",
			StringID:  user.Name,
			HasParent: true,
		},
	}
}

func NewUser(user User) *User {
	return &User{
		Name:  user.Name,
		Email: user.Email,
		SSN: user.SSN,
		keySpec: &appx.KeySpec{
			Kind:      "Users",
			StringID:  user.Name,
			HasParent: false,
		},
	}
}

func (user *User) KeySpec() *appx.KeySpec {
	return user.keySpec
}

func (user *User) CacheID() string {
	return user.SSN
}

func (user *User) Query() *datastore.Query {
	return datastore.NewQuery(user.keySpec.Kind).Filter("Email=", user.Email).Limit(1)
}
