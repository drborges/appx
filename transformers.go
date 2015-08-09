package appx

import (
	"appengine"
	"github.com/drborges/riversv2/rx"
)

type transformersBuilder struct {
	context rx.Context
}

func NewTransformer(context rx.Context) *transformersBuilder {
	return &transformersBuilder{context}
}

func (builder *transformersBuilder) ResolveEntityKey(gaeCtx appengine.Context) *transformer {
	return &transformer{
		riversCtx: builder.context,
		gaeCtx:    gaeCtx,
		transform: func(e Entity) bool {
			if err := NewKeyResolver(gaeCtx).Resolve(e); err != nil {
				panic(err)
			}

			return true
		},
	}
}

