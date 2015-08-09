package appx

import (
	"appengine"
	"github.com/drborges/riversv2/rx"
)

type transformer struct {
	riversCtx rx.Context
	gaeCtx    appengine.Context
	transform func(Entity) bool
}

func (transformer *transformer) Transform(in rx.InStream) rx.InStream {
	reader, writer := rx.NewStream(cap(in))

	go func() {
		defer transformer.riversCtx.Recover()
		defer close(writer)

		for {
			select {
			case <-transformer.riversCtx.Closed():
				return
			default:
				data, more := <-in
				if !more {
					return
				}

				if entity, ok := data.(Entity); ok {
					// If transform returns true we send the entity
					// downstream so the next transformers have a chance
					// to transform it
					if transformer.transform(entity) {
						writer <- entity
					}
				}
			}
		}
	}()

	return reader
}
