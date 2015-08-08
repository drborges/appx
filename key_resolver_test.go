package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"github.com/drborges/appxv2"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

type Entity struct {
	appx.Model
	keySpec *appx.KeySpec
}

func (entity Entity) KeySpec() *appx.KeySpec {
	return entity.keySpec
}

func TestKeyResolver(t *testing.T) {
	context, _ := aetest.NewContext(nil)
	defer context.Close()

	Convey("Given I have a key resolver", t, func() {
		resolver := appx.NewKeyResolver(context)

		Convey("When I resolve a key of an entity with no parent", func() {
			entity := &Entity{
				keySpec: &appx.KeySpec{
					Kind:  "Entity",
					IntID: 123,
				},
			}

			err := resolver.Resolve(entity)

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
			entity := &Entity{
				keySpec: &appx.KeySpec{
					Kind:      "Entity",
					IntID:     123,
					HasParent: true,
				},
			}
			parentKey := datastore.NewKey(context, "Parent", "key", 0, nil)
			entity.SetParentKey(parentKey)

			err := resolver.Resolve(entity)

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

		Convey("When I resolve a key of an entity whose key spec is missing kind information", func() {
			err := resolver.Resolve(&Entity{
				keySpec: &appx.KeySpec{},
			})

			Convey("Then it fails key resolution", func() {
				So(err, ShouldEqual, appx.ErrMissingEntityKind)
			})
		})

		Convey("When I resolve a key of an entity whose key spec is of an incomplete key", func() {
			err := resolver.Resolve(&Entity{
				keySpec: &appx.KeySpec{Kind: "Entity"},
			})

			Convey("Then it fails key resolution", func() {
				So(err, ShouldEqual, appx.ErrIncompleteKey)
			})
		})

		Convey("When I resolve a key of an entity whose key spec requires a parent key and it's missing", func() {
			err := resolver.Resolve(&Entity{
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
			entity := &Entity{
				keySpec: &appx.KeySpec{
					Kind:      "Entity",
					IntID:     123,
					HasParent: true,
				},
			}

			entity.SetParentKey(datastore.NewIncompleteKey(context, "parent", nil))
			err := resolver.Resolve(entity)

			Convey("Then it fails key resolution", func() {
				So(err, ShouldEqual, appx.ErrIncompleteParentKey)
			})
		})
	})
}
