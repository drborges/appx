package appx_test

import "github.com/drborges/appxv2"

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
