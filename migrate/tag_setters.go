package migrate

import (
	"github.com/neuronlabs/neuron/errors"
	"github.com/neuronlabs/neuron/mapping"

	"github.com/neuronlabs/neuron-extensions/repository/postgres/log"
)

// TagSetterFunc is the function that sets the proper column info for the given tag.
// I.e. having set the 'name' - tag key to a 'NameSetter' function allows to set the Column name with tag value.
type TagSetterFunc func(*mapping.StructField, *mapping.FieldTag) error

// TagSetterFunctions is the mapping for the tags with their TagSetterFunc.
var TagSetterFunctions = map[string]TagSetterFunc{}

// RegisterTagSetter registers the TagSetter function for given tag key.
func RegisterTagSetter(key string, setter TagSetterFunc) error {
	_, ok := TagSetterFunctions[key]
	if ok {
		log.Errorf("The TagSetter function for the key: '%s' already registered.", key)
		return errors.WrapDet(errors.ErrInternal, "tag setter function is already stored")
	}
	TagSetterFunctions[key] = setter
	return nil
}
