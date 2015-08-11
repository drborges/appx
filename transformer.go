package appx

import (
	"github.com/drborges/riversv2/rx"
)

type transformer struct {
	context   rx.Context
	transform func(Entity) bool
}

func (transformer *transformer) Transform(in rx.InStream) rx.InStream {
	reader, writer := rx.NewStream(cap(in))

	go func() {
		defer transformer.context.Recover()
		defer close(writer)

		for {
			select {
			case <-transformer.context.Closed():
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
