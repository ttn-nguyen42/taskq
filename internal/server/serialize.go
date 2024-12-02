package server

import (
	"encoding/json"
	"net/http"
)

func decode(r *http.Request, v interface{}) error {
	defer r.Body.Close()

	return json.
		NewDecoder(r.Body).
		Decode(v)
}

func encode(w http.ResponseWriter, v interface{}) error {
	return json.
		NewEncoder(w).
		Encode(v)
}
