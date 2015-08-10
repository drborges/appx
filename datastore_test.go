package appx_test

import (
	"appengine/aetest"
	"appengine/memcache"
	"github.com/drborges/appxv2"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestDatastoreLoad(t *testing.T) {
	context, _ := aetest.NewContext(nil)
	defer context.Close()

	user := &User{
		Name:  "Borges",
		Email: "drborges.cic@gmail.com",
		keySpec: &appx.KeySpec{
			Kind:      "Users",
			StringID:  "borges",
		},
	}

	Convey("Given I have a cached entity", t, func() {
		appx.NewKeyResolver(context).Resolve(user)

		cached := appx.CachedEntity{
			Entity:    user,
			Key:       user.Key(),
			ParentKey: user.ParentKey(),
		}

		memcache.JSON.Set(context, &memcache.Item{
			Key:    user.CacheID(),
			Object: cached,
		})

		Convey("When I load it with appx datastore", func() {
			userFromCache := &User{
				Email: "drborges.cic@gmail.com",
				keySpec: &appx.KeySpec{},
			}

			err := appx.NewDatastore(context).Load(userFromCache)

			Convey("Then the entity data is properly loaded", func() {
				So(err, ShouldBeNil)
				So(userFromCache.Name, ShouldEqual, user.Name)
				So(userFromCache.Key(), ShouldResemble, user.Key())
			})
		})
	})

	Convey("Given I have a lookupable entity", t, func() {
		Convey("When I load it with appx datastore", func() {
			appx.NewDatastore(context)

			Convey("Then the entity data is properly loaded", func() {
			})
		})
	})

	Convey("Given I have a queriable entity", t, func() {
		Convey("When I load it with appx datastore", func() {
			appx.NewDatastore(context)

			Convey("Then the entity data is properly loaded", func() {
			})
		})
	})
}
