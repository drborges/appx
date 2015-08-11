package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"appengine/memcache"
	"github.com/drborges/appxv2"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestDatastoreLoad(t *testing.T) {
	context, _ := aetest.NewContext(nil)
	defer context.Close()

	user := NewUser(User{
		Name:  "Borges",
		Email: "drborges.cic@gmail.com",
	})

	appx.NewKeyResolver(context).Resolve(user)

	Convey("Given I have a cached entity", t, func() {

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
			userFromCache := NewUser(User{
				Email: "drborges.cic@gmail.com",
			})

			err := appx.NewDatastore(context).Load(userFromCache)

			Convey("Then the entity data is properly loaded", func() {
				So(err, ShouldBeNil)
				So(userFromCache.Name, ShouldEqual, user.Name)
				So(userFromCache.Key(), ShouldResemble, user.Key())
			})
		})
	})

	Convey("Given I have a queriable entity", t, func() {
		datastore.Put(context, user.Key(), user)

		// Give datastore some time to index the data before querying
		time.Sleep(200 * time.Millisecond)

		Convey("When I load it with appx datastore", func() {
			userFromDatastore := NewUser(User{
				Email: "drborges.cic@gmail.com",
			})

			err := appx.NewDatastore(context).Load(userFromDatastore)

			Convey("Then the entity data is properly loaded", func() {
				So(err, ShouldBeNil)
				So(userFromDatastore.Name, ShouldEqual, user.Name)
				So(userFromDatastore.Key(), ShouldResemble, user.Key())
			})
		})
	})

	Convey("Given I have a lookupable entity", t, func() {
		datastore.Put(context, user.Key(), user)

		Convey("When I load it with appx datastore", func() {
			userFromDatastore := NewUser(User{
				Name: "Borges",
			})

			err := appx.NewDatastore(context).Load(userFromDatastore)

			Convey("Then the entity data is properly loaded", func() {
				So(err, ShouldBeNil)
				So(userFromDatastore.Name, ShouldEqual, user.Name)
				So(userFromDatastore.Key(), ShouldResemble, user.Key())
			})
		})
	})
}
