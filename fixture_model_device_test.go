package appx_test

import (
	"github.com/drborges/appx"
)

type Device struct {
	appx.Model
	ID    int64
	Owner string
}

func (device *Device) KeySpec() *appx.KeySpec {
	return &appx.KeySpec{
		Kind:  "Devices",
		IntID: device.ID,
	}
}

func (device *Device) SearchSpec() appx.SearchDocument {
	return appx.SearchDocument {
		Index: device.KeySpec().Kind,
		ID: device.EncodedKey(),
		Doc: &struct { Owner string } {
			Owner: device.Owner,
		},
	}
}
