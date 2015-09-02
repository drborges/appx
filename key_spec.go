package appx

type KeySpec struct {
	Kind       string
	IntID      int64
	StringID   string
	HasParent  bool
	Incomplete bool
}

func (spec *KeySpec) Validate(e Entity) error {
	if spec.Kind == "" {
		return ErrMissingEntityKind
	}

	if !spec.Incomplete && spec.IntID == 0 && spec.StringID == "" {
		return ErrIncompleteKey
	}

	if spec.Incomplete && (spec.StringID != "" || spec.IntID != 0) {
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
