package datastore

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	walName = "wal.log"

	// walMagic identifies the file so a wrong path fails loudly instead of
	// being parsed as an empty log.
	walMagic      = "DSWAL\x00"
	walVersion    = uint16(1)
	walHeaderSize = len(walMagic) + 2

	// frameHeaderSize is u32 payload length + u32 checksum.
	frameHeaderSize = 8

	// maxFrameSize bounds how much a corrupt length prefix can make us
	// allocate. Real frames are kilobytes.
	maxFrameSize = 64 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type opKind string

const (
	opPut    opKind = "put"
	opDelete opKind = "del"
)

// walOp is one record-level change. Field names are short because every frame
// carries them.
type walOp struct {
	Coll string          `json:"c"`
	Key  string          `json:"k"`
	Op   opKind          `json:"o"`
	Val  json.RawMessage `json:"v,omitempty"`
}

// walFrame is one committed transaction: the unit that is atomic on disk.
type walFrame struct {
	Seq      uint64            `json:"seq"`
	TS       int64             `json:"ts"`
	Ops      []walOp           `json:"ops"`
	Counters map[string]uint64 `json:"counters,omitempty"`

	// encodedSize is the on-disk size of this frame, filled in by append. It is
	// reported through CommitStats and is not part of the payload.
	encodedSize int `json:"-"`
}

// wal is the append-only log.
//
// Payloads are JSON on purpose. A binary codec would be smaller and faster, and
// at a handful of writes per minute neither of those matters — whereas being
// able to read the log with `less` when something has gone wrong matters a
// great deal. Do not "optimise" this.
type wal struct {
	f    *os.File
	path string
	size int64

	// readOnly suppresses every write, including the truncation of a damaged
	// tail during replay. A reader has no business editing a log another process
	// owns; it stops at the damage and reports it instead.
	readOnly bool
}

func openWAL(dir string) (*wal, error) {
	path := filepath.Join(dir, walName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("datastore: open wal: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("datastore: stat wal: %w", err)
	}

	w := &wal{f: f, path: path, size: info.Size()}

	if w.size < int64(walHeaderSize) {
		// New or truncated-to-nothing: (re)write the header.
		if err := w.writeHeader(); err != nil {
			f.Close()
			return nil, err
		}
		return w, nil
	}

	header := make([]byte, walHeaderSize)
	if _, err := f.ReadAt(header, 0); err != nil {
		f.Close()
		return nil, fmt.Errorf("datastore: read wal header: %w", err)
	}
	if string(header[:len(walMagic)]) != walMagic {
		f.Close()
		return nil, fmt.Errorf("datastore: %s is not a datastore log", path)
	}
	if v := binary.LittleEndian.Uint16(header[len(walMagic):]); v != walVersion {
		f.Close()
		return nil, fmt.Errorf("datastore: log version %d, this build understands %d", v, walVersion)
	}
	return w, nil
}

// openWALRead opens the log for reading only, tolerating a directory that has
// no log yet (a database that has only ever been compacted).
func openWALRead(dir string) (*wal, error) {
	path := filepath.Join(dir, walName)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return &wal{path: path, readOnly: true}, nil
		}
		return nil, fmt.Errorf("datastore: open wal: %w", err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("datastore: stat wal: %w", err)
	}

	w := &wal{f: f, path: path, size: info.Size(), readOnly: true}
	if w.size < int64(walHeaderSize) {
		return w, nil // nothing usable in it yet
	}

	header := make([]byte, walHeaderSize)
	if _, err := f.ReadAt(header, 0); err != nil {
		f.Close()
		return nil, fmt.Errorf("datastore: read wal header: %w", err)
	}
	if string(header[:len(walMagic)]) != walMagic {
		f.Close()
		return nil, fmt.Errorf("datastore: %s is not a datastore log", path)
	}
	if v := binary.LittleEndian.Uint16(header[len(walMagic):]); v != walVersion {
		f.Close()
		return nil, fmt.Errorf("datastore: log version %d, this build understands %d", v, walVersion)
	}
	return w, nil
}

func (w *wal) writeHeader() error {
	header := make([]byte, walHeaderSize)
	copy(header, walMagic)
	binary.LittleEndian.PutUint16(header[len(walMagic):], walVersion)
	if err := w.f.Truncate(0); err != nil {
		return fmt.Errorf("datastore: truncate wal: %w", err)
	}
	if _, err := w.f.WriteAt(header, 0); err != nil {
		return fmt.Errorf("datastore: write wal header: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("datastore: sync wal header: %w", err)
	}
	w.size = int64(walHeaderSize)
	return nil
}

// append writes one frame and, when sync is set, waits for it to reach disk.
//
// The frame is written with a single Write so the kernel sees it as one
// extent; a torn tail is still possible on power loss, which is exactly what
// the length prefix and checksum are there to detect.
func (w *wal) append(fr *walFrame, sync bool) error {
	payload, err := json.Marshal(fr)
	if err != nil {
		return fmt.Errorf("datastore: encode frame: %w", err)
	}
	if len(payload) > maxFrameSize {
		return fmt.Errorf("datastore: frame of %d bytes exceeds the %d limit", len(payload), maxFrameSize)
	}

	buf := make([]byte, frameHeaderSize+len(payload))
	binary.LittleEndian.PutUint32(buf[0:], uint32(len(payload)))
	binary.LittleEndian.PutUint32(buf[4:], crc32.Checksum(payload, crcTable))
	copy(buf[frameHeaderSize:], payload)
	fr.encodedSize = len(buf)

	n, err := w.f.WriteAt(buf, w.size)
	if err != nil {
		// A short write leaves a partial frame. That is safe: the next replay
		// detects it and truncates. Do not try to "repair" it here.
		w.size += int64(n)
		return fmt.Errorf("datastore: append frame: %w", err)
	}
	w.size += int64(n)

	if sync {
		if err := w.f.Sync(); err != nil {
			return fmt.Errorf("datastore: sync wal: %w", err)
		}
	}
	return nil
}

// replayResult describes what recovery found.
type replayResult struct {
	Frames    int
	LastSeq   uint64
	Truncated bool   // a damaged tail was discarded
	At        int64  // byte offset where truncation happened
	Reason    string // why, for the log line
}

// replay feeds every intact frame with a sequence above minSeq to fn, in order.
//
// It stops at the first frame that is short, fails its checksum, or arrives out
// of order, and truncates the file there. Stopping is the whole point: a log is
// a sequence of decisions, and once one is unreadable the ones after it cannot
// be trusted to apply to the state we think we have.
func (w *wal) replay(minSeq uint64, fn func(*walFrame) error) (replayResult, error) {
	var res replayResult

	if w.f == nil {
		return res, nil // read-only open of a directory with no log yet
	}

	if _, err := w.f.Seek(int64(walHeaderSize), io.SeekStart); err != nil {
		return res, fmt.Errorf("datastore: seek wal: %w", err)
	}
	r := bufio.NewReaderSize(w.f, 1<<16)

	offset := int64(walHeaderSize)
	lastSeq := minSeq
	head := make([]byte, frameHeaderSize)

	for {
		if _, err := io.ReadFull(r, head); err != nil {
			if errors.Is(err, io.EOF) {
				break // clean end
			}
			res.Truncated, res.Reason = true, "incomplete frame header"
			break
		}

		length := binary.LittleEndian.Uint32(head[0:])
		want := binary.LittleEndian.Uint32(head[4:])
		if length == 0 || length > maxFrameSize {
			res.Truncated, res.Reason = true, fmt.Sprintf("implausible frame length %d", length)
			break
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			res.Truncated, res.Reason = true, "incomplete frame payload"
			break
		}
		if got := crc32.Checksum(payload, crcTable); got != want {
			res.Truncated, res.Reason = true, fmt.Sprintf("checksum %08x, expected %08x", got, want)
			break
		}

		var fr walFrame
		if err := json.Unmarshal(payload, &fr); err != nil {
			res.Truncated, res.Reason = true, "undecodable frame payload"
			break
		}
		if fr.Seq <= lastSeq && res.Frames > 0 {
			res.Truncated, res.Reason = true, fmt.Sprintf("sequence %d after %d", fr.Seq, lastSeq)
			break
		}

		if fr.Seq > minSeq {
			if err := fn(&fr); err != nil {
				return res, fmt.Errorf("datastore: apply frame %d: %w", fr.Seq, err)
			}
		}

		lastSeq = fr.Seq
		res.Frames++
		res.LastSeq = fr.Seq
		offset += int64(frameHeaderSize) + int64(length)
	}

	if res.Truncated {
		res.At = offset
		if !w.readOnly {
			if err := w.f.Truncate(offset); err != nil {
				return res, fmt.Errorf("datastore: truncate damaged wal at %d: %w", offset, err)
			}
			if err := w.f.Sync(); err != nil {
				return res, fmt.Errorf("datastore: sync truncated wal: %w", err)
			}
		}
	}
	w.size = offset
	return res, nil
}

// reset empties the log back to its header. Called after a snapshot has been
// durably written, never before.
func (w *wal) reset() error {
	return w.writeHeader()
}

func (w *wal) sync() error {
	if w.f == nil {
		return nil
	}
	return w.f.Sync()
}

func (w *wal) close() error {
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}
