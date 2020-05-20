package serialization

import "encoding/json"

// JSON implements the rabbids.Serializer interface
type JSON struct{}

// Marshal returns the data in json format or an error
func (j *JSON) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// Name returns the name of the serialization used.
// This value is used as the ContentType value on the message
func (j *JSON) Name() string {
	return "application/json"
}
