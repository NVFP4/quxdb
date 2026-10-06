package core

// Iterator yields key-value pairs in key order, check Err once Next returns false.
type Iterator interface {
	Next() (key, value []byte, found bool)
	Err() error
}
