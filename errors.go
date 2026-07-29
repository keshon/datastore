package datastore

import "errors"

var (
	// ErrNotOpen is returned when the database is used before Open or after Close.
	ErrNotOpen = errors.New("datastore: not open")

	// ErrAlreadyOpen is returned by Open when the database is already open.
	ErrAlreadyOpen = errors.New("datastore: already open")

	// ErrLocked reports that another process holds the directory lock. Only one
	// writer may exist, which is why the admin has to live in the same process
	// as the site rather than beside it.
	ErrLocked = errors.New("datastore: directory is locked by another process")

	// ErrDuplicateKey reports a unique index violation. It surfaces from Update,
	// not from Put, because uniqueness is checked once at commit — that way a
	// transaction may legitimately swap a unique value between two records.
	ErrDuplicateKey = errors.New("datastore: unique index violation")

	// ErrNotFound reports a missing key where one was required.
	ErrNotFound = errors.New("datastore: key not found")

	// ErrReadOnly is returned when a write is attempted inside View.
	ErrReadOnly = errors.New("datastore: transaction is read-only")

	// ErrTxDone is returned when a transaction is used after it finished.
	ErrTxDone = errors.New("datastore: transaction already finished")

	// ErrEmptyKey reports an entity whose Key() returned "". Keys address
	// records in the log, so an empty one is unrecoverable rather than merely
	// odd.
	ErrEmptyKey = errors.New("datastore: entity key is empty")

	// ErrCorruptFrame reports a log frame that failed its checksum or length
	// check. Recovery truncates at that point rather than guessing.
	ErrCorruptFrame = errors.New("datastore: corrupt log frame")
)
