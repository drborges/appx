package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"github.com/drborges/appx"
	"github.com/drborges/riversv2"
	"github.com/drborges/riversv2/rx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestDeleteBatchFromDatastore(t *testing.T) {
	gaeCtx, _ := aetest.NewContext(nil)
	defer gaeCtx.Close()

	Convey("Given I have a load batch from datastore transformer", t, func() {
		riversCtx := rivers.NewContext()
		deleteBatchProcessor := appx.NewStep(riversCtx).DeleteBatchFromDatastore(gaeCtx)

		Convey("And I have a few entities in datastore", func() {
			user1 := NewUser(User{
				Name:  "Borges",
				Email: "borges@email.com",
				SSN:   "123123123",
			})

			user2 := NewUser(User{
				Name:  "Borges",
				Email: "borges@email.com",
				SSN:   "123123123",
			})

			err := appx.NewKeyResolver(gaeCtx).Resolve(user1)
			So(err, ShouldBeNil)
			err = appx.NewKeyResolver(gaeCtx).Resolve(user2)
			So(err, ShouldBeNil)

			_, err = datastore.Put(gaeCtx, user1.Key(), user1)
			So(err, ShouldBeNil)
			_, err = datastore.Put(gaeCtx, user2.Key(), user2)
			So(err, ShouldBeNil)

			Convey("When I transform the incoming batch", func() {
				userFromDatastore1 := NewUser(User{Name: user1.Name})
				userFromDatastore2 := NewUser(User{Name: user2.Name})
				appx.NewKeyResolver(gaeCtx).Resolve(userFromDatastore1)
				appx.NewKeyResolver(gaeCtx).Resolve(userFromDatastore2)

				batch := &appx.DatastoreBatch{
					Size: 2,
					Keys: []*datastore.Key{
						userFromDatastore1.Key(),
						userFromDatastore2.Key(),
					},
					Items: []appx.Entity{
						userFromDatastore1,
						userFromDatastore2,
					},
				}

				in, out := rx.NewStream(1)
				deleteBatchProcessor(batch, out)
				close(out)

				Convey("Then no entities are sent downstream", func() {
					So(in.Read(), ShouldBeEmpty)

					Convey("And entities are loaded from datastore", func() {
						err := datastore.Get(gaeCtx, user1.Key(), user1)
						So(err, ShouldEqual, datastore.ErrNoSuchEntity)

						err = datastore.Get(gaeCtx, user2.Key(), user2)
						So(err, ShouldEqual, datastore.ErrNoSuchEntity)
					})
				})
			})
		})
	})
}
