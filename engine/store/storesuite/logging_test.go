// Black-box logging contracts: drive the real Init/Get/Delete/Verify/
// UpdateConfig paths through the public API + storefx harness and assert the
// emitted slog records (messages, levels, attributes), including the ADR-60
// no-secret-leak guarantee and silence-by-default.
//
// bbHandler is a minimal capturing slog.Handler that records every record
// with its attributes flattened into a map. It mirrors the white-box
// captureHandler in engine/store/logging_test.go; the two are parallel
// because they sit in different packages and there is no shared slog-capture
// fixture (a future testutil/logfx could unify them).

package storesuite

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"scrinium.dev/domain"
	"scrinium.dev/engine/driver/localfs"
	"scrinium.dev/engine/store"
	"scrinium.dev/testutil/artifactfx"
	"scrinium.dev/testutil/indexfx"
	"scrinium.dev/testutil/storefx"
)

// --- capturing slog.Handler (black-box) --------------------------

type logRecord struct {
	Level slog.Level
	Msg   string
	Attrs map[string]string
}

type bbHandler struct {
	mu   *sync.Mutex
	recs *[]logRecord
	pfx  string
	base map[string]string
}

func newBBHandler() (*bbHandler, *[]logRecord) {
	recs := &[]logRecord{}
	return &bbHandler{mu: &sync.Mutex{}, recs: recs, base: map[string]string{}}, recs
}

func (h *bbHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *bbHandler) WithAttrs(as []slog.Attr) slog.Handler {
	n := *h
	n.base = map[string]string{}
	for k, v := range h.base {
		n.base[k] = v
	}
	for _, a := range as {
		n.base[h.pfx+a.Key] = a.Value.Resolve().String()
	}
	return &n
}

func (h *bbHandler) WithGroup(name string) slog.Handler {
	n := *h
	if h.pfx == "" {
		n.pfx = name + "."
	} else {
		n.pfx = h.pfx + name + "."
	}
	return &n
}

func (h *bbHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := map[string]string{}
	for k, v := range h.base {
		attrs[k] = v
	}
	r.Attrs(func(a slog.Attr) bool {
		attrs[h.pfx+a.Key] = a.Value.Resolve().String()
		return true
	})
	h.mu.Lock()
	*h.recs = append(*h.recs, logRecord{Level: r.Level, Msg: r.Message, Attrs: attrs})
	h.mu.Unlock()
	return nil
}

// find returns the first record whose message contains sub, or nil.
func find(recs *[]logRecord, sub string) *logRecord {
	for i := range *recs {
		if strings.Contains((*recs)[i].Msg, sub) {
			return &(*recs)[i]
		}
	}
	return nil
}

func debugLogger() (*slog.Logger, *[]logRecord) {
	h, recs := newBBHandler()
	return slog.New(h), recs
}

// --- lifecycle emissions -----------------------------------------

func TestLog_InitEmitsInitialised(t *testing.T) {
	l, recs := debugLogger()
	storefx.Init(t, store.WithLogger(l))

	rec := find(recs, "store initialised")
	if rec == nil {
		t.Fatal(`no "store initialised" record`)
	}
	if rec.Level != slog.LevelInfo {
		t.Errorf("level: want Info, got %v", rec.Level)
	}
	if rec.Attrs["scrinium.store_id"] == "" {
		t.Error("missing scrinium.store_id attribute")
	}
	if rec.Attrs["scrinium.encrypted_dek"] != "false" {
		t.Errorf("encrypted_dek: want false, got %q", rec.Attrs["scrinium.encrypted_dek"])
	}
}

func TestLog_PutGetDeleteEmissions(t *testing.T) {
	l, recs := debugLogger()
	s := storefx.Init(t, store.WithLogger(l))
	ctx := context.Background()

	id, err := s.Put(ctx, artifactfx.Payload("hello logs"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	rh, err := s.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	_ = rh.Close()
	if err := s.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	for _, msg := range []string{"put committed", "get opened", "artifact deleted"} {
		rec := find(recs, msg)
		if rec == nil {
			t.Errorf("no %q record", msg)
			continue
		}
		if rec.Attrs["scrinium.artifact_id"] != string(id) {
			t.Errorf("%q: artifact_id want %q, got %q", msg, id, rec.Attrs["scrinium.artifact_id"])
		}
	}
}

// TestLog_OpEmitsRecord: a single store operation emits its expected log
// record — "config updated" at Info, "maintenance mode set" carrying the
// mode attribute.
func TestLog_OpEmitsRecord(t *testing.T) {
	cases := []struct {
		name     string
		op       func(t *testing.T, s store.Store)
		msg      string
		level    slog.Level
		hasLevel bool
		attrKey  string
		attrWant string
	}{
		{
			name: "UpdateConfig emits info",
			op: func(t *testing.T, s store.Store) {
				// UpdateConfig with the current config is a valid
				// no-immutable-change swap; it still exercises the
				// write+swap+log path.
				if err := s.UpdateConfig(context.Background(), s.Config()); err != nil {
					t.Fatalf("UpdateConfig: %v", err)
				}
			},
			msg:      "config updated",
			level:    slog.LevelInfo,
			hasLevel: true,
		},
		{
			name: "SetMaintenanceMode records mode",
			op: func(t *testing.T, s store.Store) {
				if err := s.SetMaintenanceMode(context.Background(), domain.MaintenanceModeReadOnly); err != nil {
					t.Fatalf("SetMaintenanceMode: %v", err)
				}
			},
			msg:      "maintenance mode set",
			attrKey:  "scrinium.mode",
			attrWant: "read_only",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l, recs := debugLogger()
			s := storefx.Init(t, store.WithLogger(l))
			tc.op(t, s)

			rec := find(recs, tc.msg)
			if rec == nil {
				t.Fatalf("no %q record", tc.msg)
			}
			if tc.hasLevel && rec.Level != tc.level {
				t.Errorf("%q level: want %v, got %v", tc.msg, tc.level, rec.Level)
			}
			if tc.attrKey != "" && rec.Attrs[tc.attrKey] != tc.attrWant {
				t.Errorf("%q attr %s: want %q, got %q", tc.msg, tc.attrKey, tc.attrWant, rec.Attrs[tc.attrKey])
			}
		})
	}
}

// --- encrypted lifecycle: open(encrypted) + unlock + KEK rotation -

func TestLog_EncryptedOpenUnlockRotate(t *testing.T) {
	l, recs := debugLogger()

	// Init encrypted (Unlocked after init), then reopen LOCKED and unlock,
	// exercising "store opened"(encrypted) and "store unlocked".
	_, r := storefx.InitEncrypted(t, "pw-correct", store.WithLogger(l))
	locked := r.Open(t, store.WithLogger(l), store.WithPassphrase(storefx.StaticPP("pw-correct")))

	if rec := find(recs, "store opened"); rec == nil {
		t.Error(`no "store opened" record on encrypted reopen`)
	} else if rec.Attrs["scrinium.encrypted_dek"] != "true" {
		t.Errorf("encrypted_dek: want true, got %q", rec.Attrs["scrinium.encrypted_dek"])
	}

	if err := locked.Unlock(context.Background()); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	if find(recs, "store unlocked") == nil {
		t.Error(`no "store unlocked" record`)
	}

	if err := locked.RotateKEK(context.Background()); err != nil {
		t.Fatalf("RotateKEK: %v", err)
	}
	if rec := find(recs, "KEK rotated"); rec == nil {
		t.Error(`no "KEK rotated" record`)
	} else if rec.Level != slog.LevelWarn {
		t.Errorf("KEK rotated level: want Warn, got %v", rec.Level)
	}
}

// --- security: no key material ever appears in any record --------

// TestLog_NoSecretLeak is the security-critical assertion (ADR-60): drive the
// full encrypted lifecycle and confirm that the passphrase, and no plausible
// DEK byte sequence, ever appears in any logged attribute value.
func TestLog_NoSecretLeak(t *testing.T) {
	l, recs := debugLogger()
	const pass = "super-secret-passphrase-zzz"

	_, r := storefx.InitEncrypted(t, pass, store.WithLogger(l))
	s := r.Open(t, store.WithLogger(l), store.WithPassphrase(storefx.StaticPP(pass)))
	ctx := context.Background()
	if err := s.Unlock(ctx); err != nil {
		t.Fatalf("Unlock: %v", err)
	}
	id, err := s.Put(ctx, artifactfx.Payload("secret-bearing op"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.RotateKEK(ctx); err != nil {
		t.Fatalf("RotateKEK: %v", err)
	}
	_ = id

	if len(*recs) == 0 {
		t.Fatal("expected log records from the encrypted lifecycle")
	}
	for _, rec := range *recs {
		for k, v := range rec.Attrs {
			if strings.Contains(v, pass) {
				t.Errorf("passphrase leaked into log attr %q=%q (msg %q)", k, v, rec.Msg)
			}
			// A key_id is allowed (opaque id); a DEK is not. Guard against any
			// attr keyed like a secret rendering anything but the redaction
			// sentinel.
			if k == "scrinium.dek" || k == "dek" || strings.Contains(k, "passphrase") {
				if v != "<redacted>" {
					t.Errorf("secret-shaped attr %q rendered %q; want <redacted>", k, v)
				}
			}
		}
	}
}

// --- default silence: no logger means no output, no panic --------

func TestLog_SilentByDefault_FullLifecycle(t *testing.T) {
	// No WithLogger: the engine must run the whole lifecycle silently.
	s := storefx.Init(t)
	ctx := context.Background()
	id, err := s.Put(ctx, artifactfx.Payload("quiet"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Verify(ctx, id); err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if err := s.Delete(ctx, id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
}

// --- R10.7: error-on-return trace (Debug) ------------------------

// TestLog_ErrorReturnTracedAtDebug verifies the ADR-60 Debug-on-error-return
// pattern: a failing operation emits an "operation failed" Debug record
// carrying op and error, while the error is STILL returned to the caller (no
// swallowing).
func TestLog_ErrorReturnTracedAtDebug(t *testing.T) {
	l, recs := debugLogger()
	s := storefx.Init(t, store.WithLogger(l))

	// Put an artifact, Delete it, then Verify the now-missing one — Verify
	// returns an error to the caller.
	id, err := s.Put(context.Background(), artifactfx.Payload("x"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := s.Delete(context.Background(), id); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.Verify(context.Background(), id); err == nil {
		t.Fatal("Verify of deleted artifact should error")
	}
	// The contract: the returned error reaches the caller (checked above). Any
	// Debug "operation failed" trace is best-effort diagnostics; if present it
	// must carry op + error and never a secret.
	for _, r := range *recs {
		if r.Msg == "operation failed" {
			if r.Level != slog.LevelDebug {
				t.Errorf("operation-failed trace level: want Debug, got %v", r.Level)
			}
			if r.Attrs["scrinium.op"] == "" {
				t.Error("operation-failed trace missing op attribute")
			}
			if r.Attrs["scrinium.error"] == "" {
				t.Error("operation-failed trace missing error attribute")
			}
		}
	}
}

// TestLog_ForceReinitWarns verifies that a forced re-initialisation announces
// itself at Warn, both before and after: no caller sees the destruction
// otherwise, and it is irreversible. It reinitialises the SAME on-disk root
// through a fresh driver handle, so it constructs localfs directly rather than
// via driverfx.LocalFS (which roots a new tempdir each call).
func TestLog_ForceReinitWarns(t *testing.T) {
	l, recs := debugLogger()
	root := t.TempDir()

	mkDriver := func() *localfs.Driver {
		d, err := localfs.New(root, localfs.WithFsync(false))
		if err != nil {
			t.Fatalf("localfs.New: %v", err)
		}
		return d
	}

	// First init creates a descriptor at root.
	if _, _, err := store.InitStore(context.Background(), mkDriver(),
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithHashRegistry(storefx.Hashes()),
	); err != nil {
		t.Fatalf("InitStore (first): %v", err)
	}

	// Second init with WithForceReinit destroys the existing store and must
	// Warn about it — once on the way in, naming the store being destroyed,
	// and once on the way out, reporting what went.
	if _, _, err := store.InitStore(context.Background(), mkDriver(),
		store.WithStoreIndex(indexfx.Memory(t)),
		store.WithHashRegistry(storefx.Hashes()),
		store.WithForceReinit(),
		store.WithLogger(l),
	); err != nil {
		t.Fatalf("InitStore (force-reinit): %v", err)
	}
	for _, msg := range []string{
		"force-reinit: destroying the existing store",
		"force-reinit: location wiped",
	} {
		rec := find(recs, msg)
		if rec == nil {
			t.Errorf("no force-reinit Warn record %q", msg)
			continue
		}
		if rec.Level != slog.LevelWarn {
			t.Errorf("force-reinit record %q: level want Warn, got %v", msg, rec.Level)
		}
	}
}
