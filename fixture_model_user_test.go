package appx_test

import (
	"appengine/datastore"
	"github.com/drborges/appx"
)

type User struct {
	appx.Model
	Name    string
	Email   string
	SSN     string
	keySpec *appx.KeySpec
}

func NewUser(user User) *User {
	return &User{
		Name:  user.Name,
		Email: user.Email,
		SSN:   user.SSN,
		keySpec: &appx.KeySpec{
			Kind:      "Users",
			StringID:  user.Name,
			HasParent: false,
		},
	}
}

func NewUserWithFakeKey(user User) *User {
	u := &User{
		Name:  user.Name,
		Email: user.Email,
		SSN:   user.SSN,
		keySpec: &appx.KeySpec{
			Kind:      "Users",
			StringID:  user.Name,
			HasParent: false,
		},
	}
	u.SetKey(&datastore.Key{})
	return u
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
