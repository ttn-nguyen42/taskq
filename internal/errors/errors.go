package errs

import "fmt"

var (
	ErrNotFound      = fmt.Errorf("not found")
	ErrAlreadyExists = fmt.Errorf("already exists")
)

func NewErrNotFound(kind string) error {
	return fmt.Errorf("%s %w", kind, ErrNotFound)
}

func NewErrAlreadyExists(kind string) error {
	return fmt.Errorf("%s %w", kind, ErrAlreadyExists)
}
