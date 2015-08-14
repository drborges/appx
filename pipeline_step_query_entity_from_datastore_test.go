package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"github.com/drborges/appx"
	"github.com/drborges/rivers"
	"github.com/drborges/rivers/rx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
)

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
		queryProcessor := appx.NewStep(riversCtx).QueryEntityFromDatastore(gaeCtx)

		Convey("When I transform the inbound stream with non existent entity", func() {
			nonExistentUser := &User{
				Email: "diego@email.com",
				keySpec: &appx.KeySpec{
					Kind: "Users",
				},
			}

			runQuery := func () {
				queryProcessor(nonExistentUser, nil)
			}

			Convey("Then query processor panics", func() {
				So(runQuery, ShouldPanic)
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
				queryProcessor(userFromDatastore, out)
				close(out)


				Convey("Then no entities are sent downstream", func() {
					So(in.Read(), ShouldBeEmpty)

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
