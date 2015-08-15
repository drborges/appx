package appx_test

import (
	"appengine/aetest"
	"appengine/datastore"
	"fmt"
	"github.com/drborges/appx"
	. "github.com/smartystreets/goconvey/convey"
	"testing"
	"time"
	"appengine/search"
)

func TestLoadOver1000Entities(t *testing.T) {
	context, _ := aetest.NewContext(nil)
	defer context.Close()

	Convey("Given I have tons of entities in datastore", t, func() {
		datastoreUsers := []appx.Entity{}
		for i := 0; i < 1200; i++ {
			datastoreUsers = append(datastoreUsers, NewUser(User{
				Name:  fmt.Sprintf("borges %v", i),
				Email: fmt.Sprintf("borges%v@email.com", i),
				SSN:   fmt.Sprintf("SSN %v", i),
			}))
		}
		db := appx.NewDatastore(context)
		err := db.Save(datastoreUsers...)
		So(err, ShouldBeNil)

		Convey("And some entities only in the cache", func() {
			for i := 0; i < 100; i++ {
				err := datastore.Delete(context, datastoreUsers[i].Key())
				So(err, ShouldBeNil)
			}

			// Give datastore some time to index data
			time.Sleep(5 * time.Second)

			Convey("When I load all of them with appx", func() {
				loadedUsers := []appx.Entity{}
				// from cache
				for i := 0; i < 100; i++ {
					loadedUsers = append(loadedUsers, NewUser(User{
						SSN: fmt.Sprintf("SSN %v", i),
					}))
				}

				// by querying in datastore
				for i := 100; i < 200; i++ {
					loadedUsers = append(loadedUsers, NewUser(User{
						Email: fmt.Sprintf("borges%v@email.com", i),
					}))
				}

				// by datastore key lookup
				for i := 200; i < 1200; i++ {
					loadedUsers = append(loadedUsers, NewUser(User{
						Name: fmt.Sprintf("borges %v", i),
					}))
				}

				err := db.Load(loadedUsers...)

				Convey("Then all entities are loaded accordingly", func() {
					So(err, ShouldBeNil)

					for i := 0; i < 1200; i++ {
						So(loadedUsers[i].(*User).Name, ShouldEqual, datastoreUsers[i].(*User).Name)
						So(loadedUsers[i].(*User).Email, ShouldEqual, datastoreUsers[i].(*User).Email)
						So(loadedUsers[i].(*User).SSN, ShouldEqual, datastoreUsers[i].(*User).SSN)
						So(loadedUsers[i].Key(), ShouldResemble, datastoreUsers[i].Key())
					}
				})
			})
		})
	})
}

func TestLoadMultiEntityKind(t *testing.T) {
	context, _ := aetest.NewContext(nil)
	defer context.Close()

	Convey("Given I have users and devices in datastore", t, func() {
		user1 := NewUser(User{
			Name:  "borges",
			Email: "borges@email.com",
		})

		user2 := NewUser(User{
			Name:  "diego",
			Email: "diego@email.com",
		})

		device1 := &Device{
			ID:    1,
			Owner: "borges",
		}

		device2 := &Device{
			ID:    2,
			Owner: "diego",
		}

		db := appx.NewDatastore(context)
		err := db.Save(user1, device1, user2, device2)
		So(err, ShouldBeNil)

		Convey("When I load them all with appx", func() {
			user1FromDatastore := NewUser(User{Name: "borges"})
			user2FromDatastore := NewUser(User{Name: "diego"})
			device1FromDatastore := &Device{ID: 1}
			device2FromDatastore := &Device{ID: 2}

			err := db.Load(
				user1FromDatastore,
				device1FromDatastore,
				user2FromDatastore,
				device2FromDatastore)

			So(err, ShouldBeNil)
			So(user1FromDatastore, ShouldResemble, user1)
			So(user2FromDatastore, ShouldResemble, user2)
			So(device1FromDatastore, ShouldResemble, device1)
			So(device2FromDatastore, ShouldResemble, device2)
		})

		index, err := search.Open("Devices")
		So(err, ShouldBeNil)
		iter := index.Search(context, "borges", nil)
		var device Device
		_, err = iter.Next(&device)
		So(err, ShouldBeNil)
		So(device.Owner, ShouldEqual, "borges")
	})
}
