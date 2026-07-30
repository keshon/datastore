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

	// ErrUniqueViolation reports a unique index violation. It surfaces from
	// Update, not from Put, because uniqueness is checked once at commit — that
	// way a transaction may legitimately swap a unique value between two records.
	ErrUniqueViolation = errors.New("datastore: unique index violation")

	// ErrReadOnly is returned when a write is attempted inside View, or against a
	// database opened with OpenReader.
	ErrReadOnly = errors.New("datastore: transaction is read-only")

	// ErrTxDone is returned when a transaction is used after it finished.
	ErrTxDone = errors.New("datastore: transaction already finished")

	// ErrEmptyKey reports an entity whose Key() returned "". Keys address
	// records in the log, so an empty one is unrecoverable rather than merely
	// odd.
	ErrEmptyKey = errors.New("datastore: entity key is empty")

	// ErrInvalidLog reports that wal.log in the directory is not a datastore
	// log — its magic bytes are wrong. The usual cause is pointing Options.Dir
	// at a directory that belongs to something else; Open refuses rather than
	// overwrite a stranger's file.
	ErrInvalidLog = errors.New("datastore: file is not a datastore log")

	// ErrLogVersion reports a log whose format version this build does not
	// understand — the data was written by a newer datastore. Upgrade the
	// binary rather than the other way around: an older build cannot know what
	// a later format means.
	ErrLogVersion = errors.New("datastore: log format version not supported by this build")

	// ErrCorruptFrame reports a damaged frame in an archived log segment,
	// surfaced by History. Damage in the live log is handled by recovery —
	// truncated and reported through RecoveryReport — but an archive was written
	// complete, so a frame that no longer reads back is corruption, not a torn
	// tail, and pretending history simply ends there would hide it.
	ErrCorruptFrame = errors.New("datastore: corrupt log frame")

	// ErrWriteTimeout is returned by Update or Compact when the writer slot did
	// not free up within Options.WriteTimeout. The usual cause is a transaction
	// that never finishes — most often Update called from inside another Update
	// on a different goroutine, or a callback that blocks forever.
	ErrWriteTimeout = errors.New("datastore: timed out waiting for the writer slot")

	// ErrFailed is returned by every write once a log write or sync has failed.
	//
	// After a failed append the log may hold a torn frame; anything written
	// after it would be silently discarded by the next replay while this
	// process kept acknowledging commits. Refusing further writes is what keeps
	// "Update returned nil" meaning "this will survive a crash". Reopening the
	// database recovers whatever the log still holds.
	ErrFailed = errors.New("datastore: a log write failed, no further writes accepted")
)

// errDirRequired is shared by Open and OpenReader.
var errDirRequired = errors.New("datastore: Options.Dir is required")

// ErrDuplicateKey is the old name of ErrUniqueViolation.
//
// Deprecated: use ErrUniqueViolation. The old name suggested writing an
// existing primary key was an error, which it never was — Put is an upsert.
var ErrDuplicateKey = ErrUniqueViolation
