package appx_test

import (
	"appengine/aetest"
	"github.com/drborges/appxv2"
	"github.com/drborges/riversv2"
	"github.com/drborges/riversv2/rx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
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
