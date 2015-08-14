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
