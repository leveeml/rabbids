package serialization

import "encoding/json"

type JSON struct{}

func (j *JSON) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (j *JSON) Name() string {
	return "application/json"
}
