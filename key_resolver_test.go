package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"github.com/drborges/appx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestKeyResolver(t *testing.T) {
	context, _ := aetest.NewContext(nil)
	defer context.Close()

	Convey("Given I have a key manager", t, func() {
		manager := appx.NewKeyResolver(context)

		Convey("When I resolve a key of an entity with no parent", func() {
			entity := &User{
				keySpec: &appx.KeySpec{
					Kind:  "Entity",
					IntID: 123,
				},
			}

			err := manager.Resolve(entity)

			Convey("Then it succeeds", func() {
				So(err, ShouldBeNil)

				Convey("And the resolved key is set back to the entity", func() {
					So(entity.Key().StringID(), ShouldEqual, "")
					So(entity.Key().IntID(), ShouldEqual, entity.keySpec.IntID)
					So(entity.Key().Kind(), ShouldEqual, entity.keySpec.Kind)
					So(entity.Key().Parent(), ShouldBeNil)
				})
			})
		})

		Convey("When I resolve a key of an entity with parent", func() {
			entity := &User{
				keySpec: &appx.KeySpec{
					Kind:      "Entity",
					IntID:     123,
					HasParent: true,
				},
			}
			parentKey := datastore.NewKey(context, "Parent", "key", 0, nil)
			entity.SetParentKey(parentKey)

			err := manager.Resolve(entity)

			Convey("Then it succeeds", func() {
				So(err, ShouldBeNil)

				Convey("And the resolved key is set back to the entity", func() {
					So(entity.Key().Kind(), ShouldEqual, entity.keySpec.Kind)
					So(entity.Key().IntID(), ShouldEqual, entity.keySpec.IntID)
					So(entity.Key().StringID(), ShouldEqual, "")

					So(entity.Key().Parent().Kind(), ShouldEqual, parentKey.Kind())
					So(entity.Key().Parent().IntID(), ShouldEqual, 0)
					So(entity.Key().Parent().StringID(), ShouldEqual, parentKey.StringID())
				})
			})
		})

		Convey("When I resolve a key of an entity whose key spec is incomplete", func() {
			entity := &User{
				keySpec: &appx.KeySpec{
					Kind:       "People",
					Incomplete: true,
				},
			}

			err := manager.Resolve(entity)

			Convey("Then key is resolved as incomplete", func() {
				So(err, ShouldBeNil)
				So(entity.Key().Incomplete(), ShouldBeTrue)
			})
		})

		Convey("When I resolve a key of an entity whose key spec is missing kind information", func() {
			err := manager.Resolve(&User{
				keySpec: &appx.KeySpec{},
			})

			Convey("Then it fails key resolution", func() {
				So(err, ShouldEqual, appx.ErrMissingEntityKind)
			})
		})

		Convey("When I resolve a key of an entity whose key spec is of an incomplete key", func() {
			err := manager.Resolve(&User{
				keySpec: &appx.KeySpec{Kind: "Entity"},
			})

			Convey("Then it fails key resolution", func() {
				So(err, ShouldEqual, appx.ErrIncompleteKey)
			})
		})

		Convey("When I resolve a key of an entity whose key spec requires a parent key and it's missing", func() {
			err := manager.Resolve(&User{
				keySpec: &appx.KeySpec{
					Kind:      "Entity",
					IntID:     123,
					HasParent: true,
				},
			})

			Convey("Then it fails key resolution", func() {
				So(err, ShouldEqual, appx.ErrMissingParentKey)
			})
		})

		Convey("When I resolve a key of an entity whose key spec requires a parent key and parent key is incomplete", func() {
			entity := &User{
				keySpec: &appx.KeySpec{
					Kind:      "Entity",
					IntID:     123,
					HasParent: true,
				},
			}

			entity.SetParentKey(datastore.NewIncompleteKey(context, "parent", nil))
			err := manager.Resolve(entity)

			Convey("Then it fails key resolution", func() {
				So(err, ShouldEqual, appx.ErrIncompleteParentKey)
			})
		})
	})
}
