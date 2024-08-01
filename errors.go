package minibitcask

import "errors"

var (
	ErrEmptyKey      = errors.New("empty key")
	ErrKeyNotFound   = errors.New("key not found")
	ErrInvalidDBFile = errors.New("invalid db file")
)
