package core

type Cursor interface {
	Next() (key, value []byte, found bool)
	Err() error
}
