package postgres

import (
	"strings"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/migrate"
	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/mapping"
	"github.com/neuronlabs/neuron/query"
)

/**

PRIVATE FUNCTIONS

*/

func (p *Postgres) prepareInsertFieldset(modelStruct *mapping.ModelStruct, set mapping.FieldSet) (fieldSet mapping.FieldSet, autoSelected mapping.FieldSet) {
	// Trim omit keys.
	for _, field := range set {
		if field.DatabaseSkip() {
			continue
		}
		fieldSet = append(fieldSet, field)
	}
	if p.SelectNotNullsOnInsert {
		for _, field := range modelStruct.Fields() {
			if field.Kind() == mapping.KindPrimary {
				continue
			}
			if field.DatabaseNotNull() && (fieldSet == nil || !fieldSet.Contains(field)) {
				fieldSet = append(fieldSet, field)
				autoSelected = append(autoSelected, field)
			}
		}
	}
	if fieldSet != nil {
		fieldSet.Sort()
	}
	return fieldSet, autoSelected
}

func (p *Postgres) prepareUpdateModelFieldSet(set mapping.FieldSet) (fieldSet mapping.FieldSet, err error) {
	for _, field := range set {
		if field.DatabaseSkip() {
			continue
		}
		if field.Kind() == mapping.KindPrimary {
			continue
		}
		fieldSet = append(fieldSet, field)
	}

	if len(fieldSet) == 0 {
		return nil, errors.Wrap(query.ErrNoFieldsInFieldSet, "nothing to insert")
	}
	fieldSet.Sort()

	return fieldSet, err
}

func (p *Postgres) selectFieldsetNotNulls(s *query.Scope, fieldSet *mapping.FieldSet) {
	if len(*fieldSet) == len(s.ModelStruct.Fields()) {
		return
	}
	for _, field := range s.ModelStruct.Fields() {
		if fieldSet.Contains(field) {
			continue
		}
		if field.DatabaseNotNull() {
			*fieldSet = append(*fieldSet, field)
		}
	}
}

func (p *Postgres) writeQuotedWord(b *strings.Builder, word string) {
	nameType, ok := p.keywords[word]
	if !ok {
		b.WriteString(word)
		return
	}
	switch nameType {
	case migrate.KWUnreservedC, migrate.KWReservedR, migrate.KWReservedT:
		b.WriteRune('"')
		b.WriteString(word)
		b.WriteRune('"')
	default:
		b.WriteString(word)
	}
}
