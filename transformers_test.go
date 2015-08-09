package appx_test

import (
	"appengine/aetest"
	"appengine/memcache"
	"github.com/drborges/appxv2"
	"github.com/drborges/riversv2"
	"github.com/drborges/riversv2/rx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"appengine/datastore"
)

func TestResolveEntityKey(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	var validEntity1 = &Entity{
		keySpec: &appx.KeySpec{
			Kind:  "Entity",
			IntID: 123,
		},
	}

	var invalidEntity = &Entity{
		keySpec: &appx.KeySpec{
			Kind: "Entity",
		},
	}

	var validEntity2 = &Entity{
		keySpec: &appx.KeySpec{
			Kind:  "Entity",
			IntID: 321,
		},
	}

	Convey("Given I have a resolve entity key transformer", t, func() {
		riversCtx := rivers.NewContext()
		resolver := appx.NewTransformer(riversCtx).ResolveEntityKey(gaeCtx)

		Convey("When I transform entities with valid key spec from the inbound stream", func() {
			in, out := rx.NewStream(2)
			out <- validEntity1
			out <- validEntity2
			close(out)

			stream := resolver.Transform(in)

			Convey("Then all entities are transformed", func() {
				So(stream.Read(), ShouldResemble, []rx.T{
					validEntity1,
					validEntity2,
				})

				Convey("And entities have their keys resolved", func() {
					So(validEntity1.Key(), ShouldNotBeNil)
					So(validEntity2.Key(), ShouldNotBeNil)
				})
			})
		})

		Convey("When I transform entities with valid and invalid key spec from the inbound stream", func() {
			in, out := rx.NewStream(3)
			out <- validEntity1
			out <- invalidEntity
			out <- validEntity2
			close(out)

			stream := resolver.Transform(in)

			Convey("Then only entities up until the invalid entity are transformed", func() {
				So(stream.Read(), ShouldResemble, []rx.T{
					validEntity1,
				})

				Convey("And entities have their keys resolved", func() {
					So(validEntity1.Key(), ShouldNotBeNil)
					So(invalidEntity.Key(), ShouldBeNil)
					So(validEntity2.Key(), ShouldNotBeNil)

					Convey("And rivers context is closed", func() {
						_, opened := <-riversCtx.Closed()
						So(opened, ShouldBeFalse)
					})
				})
			})
		})
	})
}

func TestLoadEntityFromCache(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	var cachedUser = &User{
		Name:  "Borges",
		Email: "drborges.cic@gmail.com",
		keySpec: &appx.KeySpec{
			Kind:     "Users",
			StringID: "borges",
			HasParent: true,
		},
	}

	cachedUser.SetParentKey(datastore.NewKey(gaeCtx, "Parent", "parent id", 0, nil))

	Convey("Given I have a load entity entity from cache transformer", t, func() {
		riversCtx := rivers.NewContext()
		loader := appx.NewTransformer(riversCtx).LoadEntityFromCache(gaeCtx)

		Convey("And I have a cached entity", func() {
			appx.NewKeyResolver(gaeCtx).Resolve(cachedUser)

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
				var userFromCache = &User{
					keySpec: &appx.KeySpec{
						StringID: "borges",
					},
				}

				var userNotCached = &User{
					keySpec: &appx.KeySpec{
						StringID: "not cached",
					},
				}

				var notCacheable = &Entity{
					keySpec: &appx.KeySpec{},
				}

				in, out := rx.NewStream(3)
				out <- userFromCache
				out <- userNotCached
				out <- notCacheable
				close(out)

				stream := loader.Transform(in)

				Convey("Then non cached entities are sent downstream", func() {
					So(<-stream, ShouldEqual, userNotCached)
					So(<-stream, ShouldEqual, notCacheable)

					_, opened := <-stream
					So(opened, ShouldBeFalse)

					Convey("And all cached entities are loaded", func() {
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
