package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"appengine/memcache"
	"github.com/drborges/appxv2"
	"github.com/drborges/riversv2"
	"github.com/drborges/riversv2/rx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

func TestResolveEntityKey(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	entityWithIntID := &Entity{
		keySpec: &appx.KeySpec{
			Kind:  "Entity",
			IntID: 123,
		},
	}

	entityWithIncompleteKey := &Entity{
		keySpec: &appx.KeySpec{
			Kind: "Entity",
		},
	}

	entityWithStringID := &Entity{
		keySpec: &appx.KeySpec{
			Kind:     "Entity",
			StringID: "321",
		},
	}

	Convey("Given I have a resolve entity key transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).ResolveEntityKey(gaeCtx)

		Convey("When I transform entities with valid key spec from the inbound stream", func() {
			in, out := rx.NewStream(3)
			out <- entityWithIntID
			out <- entityWithIncompleteKey
			out <- entityWithStringID
			close(out)

			stream := transformer.Transform(in)

			Convey("Then all entities are sent downstream", func() {
				So(stream.Read(), ShouldResemble, []rx.T{
					entityWithIntID,
					entityWithIncompleteKey,
					entityWithStringID,
				})

				Convey("And entities with complete key specs have their keys resolved", func() {
					So(entityWithIntID.Key(), ShouldNotBeNil)
					So(entityWithStringID.Key(), ShouldNotBeNil)
					So(entityWithIncompleteKey.Key(), ShouldBeNil)
				})
			})
		})

		Convey("When I transform entities with keys already resolved", func() {
			key1 := datastore.NewKey(gaeCtx, "entity", "123", 0, nil)
			key2 := datastore.NewKey(gaeCtx, "entity", "", 123, nil)
			entityWithIncompleteKey.SetKey(key1)
			entityWithStringID.SetKey(key2)

			in, out := rx.NewStream(3)
			out <- entityWithIncompleteKey
			out <- entityWithStringID
			close(out)

			stream := transformer.Transform(in)

			Convey("Then all entities are sent downstream", func() {
				So(stream.Read(), ShouldResemble, []rx.T{
					entityWithIncompleteKey,
					entityWithStringID,
				})

				Convey("And entities keys are not replaced", func() {
					So(entityWithIncompleteKey.Key(), ShouldResemble, key1)
					So(entityWithStringID.Key(), ShouldResemble, key2)
				})
			})
		})
	})
}

func TestLoadEntityFromCache(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	cachedUser := NewUserWithParent(User{
		Name:  "Borges",
		Email: "drborges.cic@gmail.com",
		SSN:   "123123123",
	})

	cachedUser.SetParentKey(datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil))
	appx.NewKeyResolver(gaeCtx).Resolve(cachedUser)

	Convey("Given I have a load entity from cache transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).LoadEntitiesFromCache(gaeCtx)

		Convey("And I have a cached entity", func() {
			cached := appx.CachedEntity{
				Entity:    cachedUser,
				Key:       cachedUser.Key(),
				ParentKey: cachedUser.ParentKey(),
			}

			memcache.JSON.Set(gaeCtx, &memcache.Item{
				Key:    cachedUser.CacheID(),
				Object: cached,
			})

			Convey("When I transform the inbound entity stream", func() {
				notCacheable := &Entity{}
				userNotCached := NewUser(User{})
				userFromCache := NewUser(User{
					SSN: cachedUser.SSN,
				})

				in, out := rx.NewStream(3)
				out <- userFromCache
				out <- userNotCached
				out <- notCacheable
				close(out)

				stream := transformer.Transform(in)

				Convey("Then non cached entities are sent downstream", func() {
					So(<-stream, ShouldEqual, userNotCached)
					So(<-stream, ShouldEqual, notCacheable)

					_, opened := <-stream
					So(opened, ShouldBeFalse)

					Convey("And all cached entities are loaded", func() {
						So(userFromCache.SSN, ShouldEqual, cachedUser.SSN)
						So(userFromCache.Name, ShouldEqual, cachedUser.Name)
						So(userFromCache.Email, ShouldEqual, cachedUser.Email)
						So(userFromCache.Key(), ShouldResemble, cachedUser.Key())
						So(userFromCache.ParentKey(), ShouldResemble, cachedUser.ParentKey())
					})
				})
			})
		})
	})
}

func TestLookupEntityFromDatastore(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	user := NewUserWithParent(User{
		Name:  "Borges",
		Email: "drborges.cic@gmail.com",
		SSN:   "123123123",
	})

	parentKey := datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil)
	user.SetParentKey(parentKey)

	Convey("Given I have a lookup entity from datastore transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).LookupEntitiesFromDatastore(gaeCtx)

		Convey("When I transform the inbound stream with entities that cannot be looked up", func() {
			userMissingKey := &User{}
			nonExistentUser := NewUser(User{
				Name: "Borges",
			})
			appx.NewKeyResolver(gaeCtx).Resolve(nonExistentUser)

			in, out := rx.NewStream(2)
			out <- nonExistentUser
			out <- userMissingKey
			close(out)

			stream := transformer.Transform(in)

			Convey("Then no entities are sent downstream ", func() {
				So(stream.Read(), ShouldResemble, []rx.T{userMissingKey})

				Convey("And context is closed with error", func() {
					_, opened := <-riversCtx.Closed()
					So(opened, ShouldBeFalse)
					So(riversCtx.Err().Error(), ShouldResemble, datastore.ErrNoSuchEntity.Error())
				})
			})
		})

		Convey("And I have an entity in datastore", func() {
			err := appx.NewKeyResolver(gaeCtx).Resolve(user)
			So(err, ShouldBeNil)

			_, err = datastore.Put(gaeCtx, user.Key(), user)
			So(err, ShouldBeNil)

			Convey("When I transform the inbound entity stream", func() {
				userFromDatastore := NewUserWithParent(User{Name: "Borges"})
				userFromDatastore.SetParentKey(parentKey)
				appx.NewKeyResolver(gaeCtx).Resolve(userFromDatastore)

				userMissingKey := NewUser(User{})
				userWithIncompleteKey := NewUser(User{})
				userWithIncompleteKey.SetKey(datastore.NewIncompleteKey(gaeCtx, "Users", nil))

				in, out := rx.NewStream(3)
				out <- userFromDatastore
				out <- userMissingKey
				out <- userWithIncompleteKey
				close(out)

				stream := transformer.Transform(in)

				Convey("Then non existent entities are sent downstream", func() {
					So(<-stream, ShouldResemble, userMissingKey)
					So(<-stream, ShouldResemble, userWithIncompleteKey)

					_, opened := <-stream
					So(opened, ShouldBeFalse)

					Convey("And all existent entities are loaded", func() {
						So(userFromDatastore.SSN, ShouldEqual, user.SSN)
						So(userFromDatastore.Name, ShouldEqual, user.Name)
						So(userFromDatastore.Email, ShouldEqual, user.Email)
						So(userFromDatastore.Key(), ShouldResemble, user.Key())
						So(userFromDatastore.ParentKey(), ShouldResemble, user.ParentKey())
					})
				})
			})
		})
	})
}

func TestQueryEntityFromDatastore(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	user := &User{
		Name:  "Borges",
		Email: "diego@email.com",
		SSN:   "123123123",
		keySpec: &appx.KeySpec{
			Kind:     "Users",
			StringID: "borges",
		},
	}

	parentKey := datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil)
	user.SetParentKey(parentKey)

	Convey("Given I have a query entity from datastore transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).QueryEntityFromDatastore(gaeCtx)

		Convey("When I transform the inbound stream with non existent entity", func() {
			userFromDatastore := &User{
				Email: "diego@email.com",
				keySpec: &appx.KeySpec{
					Kind: "Users",
				},
			}

			in, out := rx.NewStream(2)
			out <- userFromDatastore
			close(out)

			stream := transformer.Transform(in)

			Convey("Then no entities are sent downstream", func() {
				So(stream.Read(), ShouldBeEmpty)

				Convey("And context is closed with error", func() {
					_, opened := <-riversCtx.Closed()
					So(opened, ShouldBeFalse)
					So(riversCtx.Err(), ShouldNotBeNil)
				})
			})
		})

		Convey("And I have an entity in datastore", func() {
			err := appx.NewKeyResolver(gaeCtx).Resolve(user)
			So(err, ShouldBeNil)

			_, err = datastore.Put(gaeCtx, user.Key(), user)
			So(err, ShouldBeNil)

			// Give datastore some time so that the created entity is available to be queried
			time.Sleep(200 * time.Millisecond)

			Convey("When I transform the inbound stream", func() {
				userFromDatastore := &User{
					Email: "diego@email.com",
					keySpec: &appx.KeySpec{
						Kind: "Users",
					},
				}

				in, out := rx.NewStream(2)
				out <- userFromDatastore
				close(out)

				stream := transformer.Transform(in)

				Convey("Then no entities are sent downstream", func() {
					So(stream.Read(), ShouldBeEmpty)

					Convey("And queryable entities are loaded from datastore", func() {
						So(userFromDatastore.Name, ShouldEqual, user.Name)
						So(userFromDatastore.Email, ShouldEqual, user.Email)
						So(userFromDatastore.Key(), ShouldResemble, user.Key())
						So(userFromDatastore.ParentKey(), ShouldResemble, user.ParentKey())
					})
				})
			})
		})
	})
}

func TestUpdateEntitiesInDatastore(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	userMissingKey := NewUser(User{
		Name:  "Another User",
		Email: "user@email.com",
	})

	newUser := NewUser(User{
		Name:  "Diego",
		Email: "diego@email.com",
	})

	existentUser := NewUser(User{
		Name:  "Borges",
		Email: "borges@email.com",
	})

	appx.NewKeyResolver(gaeCtx).Resolve(newUser)
	appx.NewKeyResolver(gaeCtx).Resolve(existentUser)

	newUserKeyBeforeUpdate := newUser.Key()

	Convey("Given I have a update entities in datastore transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).UpdateEntitiesInDatastore(gaeCtx)

		Convey("And I have an existent entity", func() {
			datastore.Put(gaeCtx, existentUser.Key(), existentUser)

			Convey("When I transform the inbound stream", func() {
				existentUser.Name = "borges2"

				in, out := rx.NewStream(4)
				out <- existentUser
				out <- userMissingKey
				out <- newUser
				out <- "notAnEntity"
				close(out)

				stream := transformer.Transform(in)

				Convey("Then entities missing keys and non entities are sent downstream", func() {
					So(stream.Read(), ShouldResemble, []rx.T{userMissingKey, "notAnEntity"})

					Convey("And new users are created", func() {
						var userFromDatastore User
						datastore.Get(gaeCtx, newUser.Key(), &userFromDatastore)

						So(newUser.Key(), ShouldNotEqual, newUserKeyBeforeUpdate)
						So(userFromDatastore.Name, ShouldResemble, newUser.Name)
						So(userFromDatastore.Email, ShouldResemble, newUser.Email)

						Convey("And existent users are updated", func() {
							var userFromDatastore User
							datastore.Get(gaeCtx, existentUser.Key(), &userFromDatastore)

							So(userFromDatastore.Name, ShouldResemble, existentUser.Name)
							So(userFromDatastore.Email, ShouldResemble, existentUser.Email)
						})
					})
				})
			})
		})
	})
}

func TestUpdateEntitiesInCache(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	cachedUser := NewUserWithParent(User{
		Name:  "Borges",
		Email: "drborges.cic@gmail.com",
		SSN: "123123123",
	})

	newUser := NewUserWithParent(User{
		Name:  "Diego",
		Email: "diego@email.com",
		SSN: "321321321",
	})

	cachedUser.SetParentKey(datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil))
	newUser.SetParentKey(datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil))
	appx.NewKeyResolver(gaeCtx).Resolve(cachedUser)
	appx.NewKeyResolver(gaeCtx).Resolve(newUser)

	Convey("Given I have a update entities in cache transformer", t, func() {
		riversCtx := rivers.NewContext()
		transformer := appx.NewTransformer(riversCtx).UpdateEntitiesInCache(gaeCtx)

		Convey("And I have a cached entity", func() {
			cached := appx.CachedEntity{
				Entity:    cachedUser,
				Key:       cachedUser.Key(),
				ParentKey: cachedUser.ParentKey(),
			}

			memcache.JSON.Set(gaeCtx, &memcache.Item{
				Key:    cachedUser.CacheID(),
				Object: cached,
			})

			Convey("When I transform the inbound entity stream", func() {
				notCacheable := &Entity{}
				cachedUser.Name = "borges"

				in, out := rx.NewStream(4)
				out <- cachedUser
				out <- notCacheable
				out <- newUser
				out <- "notAnEntity"
				close(out)

				stream := transformer.Transform(in)

				Convey("Then data that is not cacheable and nor an entity are sent downstream", func() {
					So(<-stream, ShouldEqual, notCacheable)
					So(<-stream, ShouldEqual, "notAnEntity")

					_, opened := <-stream
					So(opened, ShouldBeFalse)

					Convey("And all cacheable entities are saved in the cache", func() {
						cachedUserFromCache := &User{}
						newUserFromCache := &User{}

						cachedEntity1 := &appx.CachedEntity{
							Entity: cachedUserFromCache,
						}

						cachedEntity2 := &appx.CachedEntity{
							Entity: newUserFromCache,
						}

						_, err := memcache.JSON.Get(gaeCtx, cachedUser.CacheID(), cachedEntity1)
						So(err, ShouldBeNil)

						_, err = memcache.JSON.Get(gaeCtx, newUser.CacheID(), cachedEntity2)
						So(err, ShouldBeNil)

						cachedEntity1.Entity.SetKey(cachedEntity1.Key)
						cachedEntity2.Entity.SetKey(cachedEntity2.Key)

						So(cachedUserFromCache.Name, ShouldResemble, cachedUser.Name)
						So(cachedUserFromCache.Email, ShouldResemble, cachedUser.Email)
						So(cachedUserFromCache.Key(), ShouldResemble, cachedUser.Key())
						So(cachedUserFromCache.ParentKey(), ShouldResemble, cachedUser.ParentKey())

						So(newUserFromCache.Name, ShouldResemble, newUser.Name)
						So(newUserFromCache.Email, ShouldResemble, newUser.Email)
						So(newUserFromCache.Key(), ShouldResemble, newUser.Key())
						So(newUserFromCache.ParentKey(), ShouldResemble, newUser.ParentKey())
					})
				})
			})
		})
	})
}
