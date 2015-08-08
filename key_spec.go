package appx

type KeySpec struct {
	Kind      string
	IntID     int64
	StringID  string
	HasParent bool
}

func (spec *KeySpec) IncompleteKey() bool {
	return spec.IntID == 0 && spec.StringID == ""
}

func (spec *KeySpec) Validate(e Entity) error {
	if spec.Kind == "" {
		return ErrMissingEntityKind
	}

	if spec.IncompleteKey() {
		return ErrIncompleteKey
	}

	if spec.HasParent && e.ParentKey() == nil {
		return ErrMissingParentKey
	}

	if spec.HasParent && e.ParentKey().Incomplete() {
		return ErrIncompleteParentKey
	}

	return nil
}
