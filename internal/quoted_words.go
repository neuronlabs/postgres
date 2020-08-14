package internal

import (
	"strings"
)

// QuotedWordWriteFunc is the writer for the quoted words.
type QuotedWordWriteFunc func(sb *strings.Builder, word string)

// DummyQuotedWriteFunc is the dummy writer for the quoted words.
func DummyQuotedWriteFunc(sb *strings.Builder, word string) {
	sb.WriteString(word)
}
