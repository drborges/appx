package templates

var AppxRepository = `// AUTO GENERATED BY appx-cli - DO NOT EDIT

package {{.Pkg}}

import (
	// TODO Need to come up with a proper way to handle conditional imports
	{{ range $field, $type := .Fields }}
	{{ if startsWith $type "time.Time" }}
	"time"
	{{ end }}
	{{ end }}
	"appengine/datastore"
	"github.com/drborges/appx"
	"github.com/drborges/rivers"
)

func From{{.Name}}(db *appx.Datastore) *{{.Name}}AppxRepository {
	return &{{.Name}}AppxRepository{
		db: db,
	}
}

type {{.Name}}AppxRepository struct {
	db *appx.Datastore
}

func (repo *{{.Name}}AppxRepository) New() *{{.Name}}AppxModel {
	return &{{.Name}}AppxModel{
		{{.Name}}: &{{.Name}}{},
		db: repo.db,
	}
}

func (repo *{{.Name}}AppxRepository) Create(data {{.Name}}) (*{{.Name}}AppxModel, error) {
	model := repo.New()
	return model, model.With(data).Save()
}

func (repo *{{.Name}}AppxRepository) Delete(data {{.Name}}) error {
	return repo.db.Delete(&data)
}

func (repo *{{.Name}}AppxRepository) GetByKey(key *datastore.Key) (*{{.Name}}AppxModel, error) {
	item := repo.New()
	item.SetKey(key)
	return item, repo.db.Load(item)
}

func (repo *{{.Name}}AppxRepository) GetByEncodedKey(key string) (*{{.Name}}AppxModel, error) {
	item := repo.New()
	if err := item.SetEncodedKey(key); err != nil {
		return nil, err
	}
	return item, repo.db.Load(item)
}

{{ $model := . }}

{{range $field, $type := .Fields}}
func (repo *{{$model.Name}}AppxRepository) GetBy{{ $field }}(value {{ $type }}) (*{{$model.Name}}AppxModel, error) {
	item := repo.New()
	item.{{ $field }} = value
	return item, repo.db.Load(item)
}
{{end}}

{{range $field, $type := .Fields}}
func (repo *{{ $model.Name }}AppxRepository) FindWhere{{ $field }}(op string, value {{ $type }}) *{{$model.Name}}QueryRunner {
	q := datastore.NewQuery(new({{ $model.Name }}).KeySpec().Kind).Filter("{{ $field }}" + op, value)
	return &{{$model.Name}}QueryRunner{
		db: repo.db,
		q:  q,
	}
}
{{end}}

{{ range $field, $type := .Fields }}
{{ if startsWith $type "[]" }}
func (repo *{{ $model.Name }}AppxRepository) FindWhere{{ $field }}Contains(value {{ sliceElmType $type }}) *{{$model.Name}}QueryRunner {
	q := datastore.NewQuery(new({{ $model.Name }}).KeySpec().Kind).Filter("{{ $field }}=", value)
	return &{{$model.Name}}QueryRunner{
		db: repo.db,
		q:  q,
	}
}
{{ end }}
{{end}}

func (repo *{{.Name}}AppxRepository) FindWhere(filter string, value interface{}) *{{$model.Name}}QueryRunner {
	q := datastore.NewQuery(new({{.Name}}).KeySpec().Kind).Filter(filter, value)
	return &{{$model.Name}}QueryRunner{
		db: repo.db,
		q:  q,
	}
}

func (repo *{{.Name}}AppxRepository) FindWhereAncestorIs(ancestor appx.Entity) *{{$model.Name}}QueryRunner {
	q := datastore.NewQuery(new({{.Name}}).KeySpec().Kind).Ancestor(ancestor.Key())
	return &{{$model.Name}}QueryRunner{
		db: repo.db,
		q:  q,
	}
}

func (repo *{{ .Name }}AppxRepository) FindBy(q *datastore.Query) *{{.Name}}QueryRunner {
	return &{{ .Name }}QueryRunner{
		db: repo.db,
		q:  q,
	}
}

func (repo *{{ .Name }}AppxRepository) FindAll() *{{.Name}}QueryRunner {
	return repo.FindBy(datastore.NewQuery(new({{ $model.Name }}).KeySpec().Kind))
}

{{range $field, $type := .Fields}}
func (repo *{{ $model.Name }}AppxRepository) FindBy{{ $field }}(value {{ $type }}) *{{$model.Name}}QueryRunner {
	q := datastore.NewQuery(new({{ $model.Name }}).KeySpec().Kind).Filter("{{ $field }}=", value)
	return &{{$model.Name}}QueryRunner{
		db: repo.db,
		q:  q,
	}
}
{{end}}

type {{$model.Name}}QueryRunner struct {
	db     *appx.Datastore
	q      *datastore.Query
	cursor string
}

func (runner *{{$model.Name}}QueryRunner) Select(fields ...string) *{{$model.Name}}QueryRunner {
	runner.q = runner.q.Project(fields...)
	return runner
}

func (runner *{{$model.Name}}QueryRunner) Distinct() *{{$model.Name}}QueryRunner {
	runner.q = runner.q.Distinct()
	return runner
}

func (runner *{{$model.Name}}QueryRunner) Limit(limit int) *{{$model.Name}}QueryRunner {
	runner.q = runner.q.Limit(limit)
	return runner
}

func (runner *{{$model.Name}}QueryRunner) KeysOnly() *{{$model.Name}}QueryRunner {
	runner.q = runner.q.KeysOnly()
	return runner
}

{{ range $field, $type := .Fields }}
func (runner *{{$model.Name}}QueryRunner) OrderBy{{ $field }}Asc() *{{$model.Name}}QueryRunner {
	runner.q = runner.q.Order("{{ $field }}")
	return runner
}
{{end}}

{{ range $field, $type := .Fields }}
func (runner *{{$model.Name}}QueryRunner) OrderBy{{ $field }}Desc() *{{$model.Name}}QueryRunner {
	runner.q = runner.q.Order("-{{ $field }}")
	return runner
}
{{end}}

func (runner *{{$model.Name}}QueryRunner) Stream() *rivers.Stage {
	return runner.db.Query(runner.q).StartFrom(runner.cursor).StreamOf({{.Name}}{})
}

func (runner *{{$model.Name}}QueryRunner) Count() (int, error) {
	return runner.db.Query(runner.q).StartFrom(runner.cursor).Count()
}

func (runner *{{$model.Name}}QueryRunner) All() ([]*{{.Name}}AppxModel, error) {
	items := []*{{.Name}}AppxModel{}
	return items, runner.db.Query(runner.q).StartFrom(runner.cursor).Results(items)
}

func (runner *{{$model.Name}}QueryRunner) AllAs(items *[]*{{.Name}}) error {
	return runner.db.Query(runner.q).StartFrom(runner.cursor).Results(items)
}

func (runner *{{$model.Name}}QueryRunner) First() (*{{.Name}}AppxModel, error) {
	item := &{{.Name}}AppxModel{}
	return item, runner.db.Query(runner.q.Limit(1)).StartFrom(runner.cursor).Result(item)
}

func (runner *{{$model.Name}}QueryRunner) FirstAs(item *{{.Name}}) error {
	return runner.db.Query(runner.q.Limit(1)).StartFrom(runner.cursor).Result(item)
}

func (runner *{{$model.Name}}QueryRunner) PagesIterator() appx.Iterator {
	return runner.db.Query(runner.q).StartFrom(runner.cursor).PagesIterator()
}

func (runner *{{$model.Name}}QueryRunner) ItemsIterator() appx.Iterator {
	return runner.db.Query(runner.q).StartFrom(runner.cursor).ItemsIterator()
}

func (runner *{{$model.Name}}QueryRunner) StartFrom(cursor string) *{{$model.Name}}QueryRunner {
	runner.cursor = cursor
	return runner
}`
