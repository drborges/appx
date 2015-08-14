package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"appengine/memcache"
	"encoding/json"
	"github.com/drborges/appx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestDatastoreLoad(t *testing.T) {
	context, _ := aetest.NewContext(nil)
	defer context.Close()

	Convey("Given I have a cached entity", t, func() {
		user := NewUser(User{
			Name:  "Borges",
			Email: "drborges.cic@gmail.com",
			SSN:   "123123123",
		})

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
			userFromCache := NewUser(User{
				SSN: user.SSN,
			})

			err := appx.NewDatastore(context).Load(userFromCache)

			Convey("Then the entity data is properly loaded", func() {
				So(err, ShouldBeNil)
				So(userFromCache.SSN, ShouldEqual, user.SSN)
				So(userFromCache.Name, ShouldEqual, user.Name)
				So(userFromCache.Email, ShouldEqual, user.Email)
				So(userFromCache.Key(), ShouldResemble, user.Key())
			})
		})
	})

	Convey("Given I have a queriable entity", t, func() {
		user := NewUser(User{
			Name:  "Borges",
			Email: "drborges.cic@gmail.com",
			SSN:   "321321",
		})

		appx.NewKeyResolver(context).Resolve(user)

		datastore.Put(context, user.Key(), user)

		// Give datastore some time to index the data before querying
		time.Sleep(200 * time.Millisecond)

		Convey("When I load it with appx datastore", func() {
			userFromDatastore := NewUser(User{
				Email: user.Email,
				SSN:   "321321",
			})

			err := appx.NewDatastore(context).Load(userFromDatastore)

			Convey("Then the entity data is properly loaded", func() {
				So(err, ShouldBeNil)
				So(userFromDatastore.SSN, ShouldEqual, user.SSN)
				So(userFromDatastore.Name, ShouldEqual, user.Name)
				So(userFromDatastore.Email, ShouldEqual, user.Email)
				So(userFromDatastore.Key(), ShouldResemble, user.Key())

				Convey("And the entity is cached in case it is cacheable", func() {
					userFromCache := NewUser(User{Name: "Borges"})
					cached := appx.CachedEntity{
						Entity: userFromCache,
					}
					item, err := memcache.Get(context, user.CacheID())
					json.Unmarshal(item.Value, &cached)
					appx.NewKeyResolver(context).Resolve(userFromCache)
					So(err, ShouldBeNil)
					So(userFromCache, ShouldResemble, user)
				})
			})
		})
	})

	Convey("Given I have a lookupable entity", t, func() {
		user := NewUser(User{
			Name:  "Borges",
			Email: "drborges.cic@gmail.com",
			SSN:   "987987",
		})

		appx.NewKeyResolver(context).Resolve(user)
		datastore.Put(context, user.Key(), user)

		Convey("When I load it with appx datastore", func() {
			userFromDatastore := NewUser(User{
				Name: "Borges",
			})

			err := appx.NewDatastore(context).Load(userFromDatastore)

			Convey("Then the entity data is properly loaded", func() {
				So(err, ShouldBeNil)
				So(userFromDatastore.SSN, ShouldEqual, user.SSN)
				So(userFromDatastore.Name, ShouldEqual, user.Name)
				So(userFromDatastore.Email, ShouldEqual, user.Email)
				So(userFromDatastore.Key(), ShouldResemble, user.Key())

				Convey("And the entity is not cached if its cache id is empty", func() {
					_, err := memcache.Get(context, user.CacheID())
					So(err, ShouldEqual, memcache.ErrCacheMiss)
				})
			})
		})
	})
}

func TestDatastoreSave(t *testing.T) {
	context, _ := aetest.NewContext(nil)
	defer context.Close()

	Convey("Given I have an entity in datastore", t, func() {
		user1 := NewUser(User{
			Name:  "Borges",
			Email: "borges@email.com",
			SSN:   "123123123",
		})

		appx.NewKeyResolver(context).Resolve(user1)
		datastore.Put(context, user1.Key(), user1)

		Convey("And I have an entity not yet saved to datastore", func() {
			user2 := NewUser(User{
				Name:  "Diego",
				Email: "diego@email.com",
				SSN:   "321321321",
			})

			Convey("When I update the entities in datastore", func() {
				err := appx.NewDatastore(context).Save(user1, user2)

				Convey("Then the entities are updated in the datastore", func() {
					So(err, ShouldBeNil)
					user1FromDatastore := &User{}
					user2FromDatastore := &User{}
					user1FromDatastore.SetKey(user1.Key())
					user2FromDatastore.SetKey(user2.Key())

					datastore.Get(context, user1.Key(), user1FromDatastore)
					datastore.Get(context, user2.Key(), user2FromDatastore)

					So(user1FromDatastore.SSN, ShouldEqual, user1.SSN)
					So(user1FromDatastore.Name, ShouldEqual, user1.Name)
					So(user1FromDatastore.Email, ShouldEqual, user1.Email)
					So(user1FromDatastore.Key(), ShouldResemble, user1.Key())

					So(user2FromDatastore.SSN, ShouldEqual, user2.SSN)
					So(user2FromDatastore.Name, ShouldEqual, user2.Name)
					So(user2FromDatastore.Email, ShouldEqual, user2.Email)
					So(user2FromDatastore.Key(), ShouldResemble, user2.Key())
				})
			})
		})
	})
}

func TestDatastoreDelete(t *testing.T) {
	context, _ := aetest.NewContext(nil)
	defer context.Close()

	Convey("Given I have an entity in datastore", t, func() {
		userInDatastore := NewUser(User{
			Name:  "Borges",
			Email: "borges@email.com",
			SSN:   "123123123",
		})

		appx.NewKeyResolver(context).Resolve(userInDatastore)
		datastore.Put(context, userInDatastore.Key(), userInDatastore)

		Convey("And I have a cached entity though not yet saved to datastore", func() {
			cachedUser := NewUser(User{
				Name:  "Diego",
				Email: "diego@email.com",
				SSN:   "321321321",
			})

			cached := appx.CachedEntity{
				Entity:    cachedUser,
				Key:       cachedUser.Key(),
				ParentKey: cachedUser.ParentKey(),
			}

			memcache.JSON.Set(context, &memcache.Item{
				Key:    cachedUser.CacheID(),
				Object: cached,
			})

			Convey("And I have a non existent user", func() {
				nonExistentUser := NewUser(User{Name: "not existent"})

				Convey("When I delete the all", func() {
					err := appx.NewDatastore(context).Delete(userInDatastore, cachedUser, nonExistentUser)

					Convey("Then the entities are deleted from cache and datastore", func() {
						So(err, ShouldBeNil)

						err := datastore.Get(context, userInDatastore.Key(), userInDatastore)
						So(err, ShouldEqual, datastore.ErrNoSuchEntity)

						_, err = memcache.Get(context, cachedUser.CacheID())
						So(err, ShouldEqual, memcache.ErrCacheMiss)
					})
				})
			})
		})
	})
}
