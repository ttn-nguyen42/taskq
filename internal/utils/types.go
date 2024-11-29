package utils

// Empty is an empty struct, which has 0 bytes.
type Empty struct{}

// UniqueSet is a set of unique item, used to check if a key is already exists
type UniqueSet map[string]Empty

func (s UniqueSet) Add(key string) {
	s[key] = Empty{}
}

func (s UniqueSet) AlreadyExists(key string) bool {
	_, exists := s[key]
	return exists
}

func (s UniqueSet) Delete(key string) {
	delete(s, key)
}
