package migrate

import (
	"context"
	"strings"
	"sync"

	"github.com/neuronlabs/neuron/errors"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/internal"
	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
)

// KeyWordType is the postgres default key word type.
type KeyWordType int

// IsReserved checks if the current keyword is reserved.
func (k KeyWordType) IsReserved() bool {
	switch k {
	case KWUnreservedC, KWReservedR, KWReservedT:
		return true
	default:
		return false
	}
}

const (
	// KWUnknown is the unknown keyword type.
	KWUnknown KeyWordType = iota
	// KWUnreservedU is the unreserved key word type.
	KWUnreservedU
	// KWUnreservedC is the unreserved key word type that cannot be a function or type name.
	KWUnreservedC
	// KWReservedR is the reserved keyword type.
	KWReservedR
	// KWReservedT is the reserved keyword that can be a function or type name.
	KWReservedT
)

type keyWordMap struct {
	keyWords map[int]map[string]KeyWordType
	sync.Mutex
}

// Get gets the keyword map for provided version.
func (k *keyWordMap) Get(version int) (map[string]KeyWordType, bool) {
	k.Lock()
	defer k.Unlock()
	m, ok := k.keyWords[version]
	return m, ok
}

// Set sets the keyword versioned map.
func (k *keyWordMap) Set(version int, m map[string]KeyWordType) {
	k.Lock()
	defer k.Unlock()
	k.keyWords[version] = m
}

var keyWordsVersions = &keyWordMap{keyWords: map[int]map[string]KeyWordType{}}

// GetQuotedWord gets the quoted 'word' if it is on the lists of the keywords.
// The postgres version 'pgVersion' is the numeric version of the postgres server.
func GetQuotedWord(word string, pgVersion int) string {
	nameType := getKeyWordType(word, pgVersion)
	switch nameType {
	case KWUnreservedC, KWReservedR, KWReservedT:
		return "\"" + word + "\""
	default:
		return word
	}
}

// GetKeyWordType gets the keyword type for given 'word' and postgres version 'pgVersion'.
// If field is not found returns 'KWUnknown'.
func GetKeyWordType(word string, pgVersion int) KeyWordType {
	return getKeyWordType(word, pgVersion)
}

// GetKeyWords gets and stores the keywords for the provided postgres 'version' from the current database 'db' connection.
func GetKeyWords(ctx context.Context, conn internal.Connection, version int) (map[string]KeyWordType, error) {
	kwMap, ok := keyWordsVersions.Get(version)
	if ok && kwMap != nil {
		return kwMap, nil
	}

	rows, err := conn.Query(ctx, "SELECT word, catcode FROM pg_get_keywords()")
	if err != nil {
		return nil, errors.WrapDetf(errors.ErrInternal, "can't get query postgres key words: %v", err.Error())
	}
	defer rows.Close()

	keywords := map[string]KeyWordType{}
	var (
		word    string
		catCode rune
		kwt     KeyWordType
	)
	for rows.Next() {
		if err = rows.Scan(&word, &catCode); err != nil {
			log.Errorf("Scanning key words query failed: %v", err)
			return nil, err
		}

		switch catCode {
		case 'T':
			kwt = KWReservedT
		case 'R':
			kwt = KWReservedR
		case 'U':
			kwt = KWUnreservedU
		case 'C':
			kwt = KWUnreservedC
		default:
			log.Errorf("Unknown keyword type: '%s'. Setting keyword: '%s' as reservedR", catCode, word)
			kwt = KWReservedR
		}
		keywords[word] = kwt
	}
	keyWordsVersions.Set(version, keywords)
	return keywords, nil
}

// WriteQuotedWord surrounds the 'word' with quotations '"' signs if it is one of reserved keywords.
// The postgres version 'pgVersion' is the numeric version of the postgres server.
// The result is being written into provided 'b' strings.Builder.
func WriteQuotedWord(b *strings.Builder, word string, pgVersion int) {
	nameType := getKeyWordType(word, pgVersion)
	switch nameType {
	case KWUnreservedC, KWReservedR, KWReservedT:
		b.WriteRune('"')
		b.WriteString(word)
		b.WriteRune('"')
	default:
		b.WriteString(word)
	}
}

func getKeyWordType(word string, pgVersion int) KeyWordType {
	kw, ok := keyWordsVersions.Get(pgVersion)
	if !ok {
		log.Debugf("No keywords set for the postgres version: '%d'", pgVersion)
		return KWUnknown
	}
	tp, ok := kw[word]
	if !ok {
		tp = KWUnknown
	}
	return tp
}

func quoteIdentifier(name string) string {
	endRune := strings.IndexRune(name, 0)
	if endRune > -1 {
		name = name[:endRune]
	}
	return `"` + strings.Replace(name, `"`, `""`, -1) + `"`
}
