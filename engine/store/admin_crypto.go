package store

import (
	"context"
	"fmt"
	"log/slog"

	store2 "scrinium.dev/config"
	"scrinium.dev/domain"
)

// admin_crypto.go — the AdminStore crypto operations. Each method is the
// whole operation: it applies the state-machine gate, performs the state
// transition and logging, and delegates the key-material work (prompting,
// KEK derivation, DEK wrapping, descriptor persist) to the crypto.State held
// on the store. The mechanics and the mutex that guards the key material
// live in engine/store/internal/crypto; this file owns only the surrounding
// state-machine concerns.
//
// Locking: the gates and transitions here take stateMu; the crypto.State
// methods take their own mutex internally and release it before returning.
// The two mutexes are never held at once.

// Unlock transitions an encrypted Store from Locked to Unlocked.
// Idempotent in Unlocked. enterAdmin allows Locked through (the whole point
// of Unlock is to leave Locked) while rejecting closed/corrupted/offline/
// bootstrapping. State checks happen before the provider is invoked, so an
// already-Unlocked Store does not prompt.
func (s *store) Unlock(ctx context.Context) error {
	if err := s.enterAdmin(ctx); err != nil {
		return err
	}

	// Claim the bootstrap transition. Only a Locked Store does work, and
	// exactly one caller wins the Locked→Bootstrapping move; a second
	// concurrent Unlock observes Bootstrapping and is refused, an
	// already-Unlocked Store is a no-op (but still logs, as before).
	s.stateMu.Lock()
	switch s.state {
	case domain.StateUnlocked:
		s.stateMu.Unlock()
	case domain.StateLocked:
		s.state = domain.StateBootstrapping
		s.stateMu.Unlock()

		// Unwrap the DEK (idempotent on the DEK; no re-prompt if already
		// held). On failure revert to Locked so a retry can re-prompt.
		if err := s.crypto.UnlockDEK(ctx, s.storeID); err != nil {
			s.stateMu.Lock()
			s.state = domain.StateLocked
			s.stateMu.Unlock()
			return fmt.Errorf("store.Unlock: %w", err)
		}

		// Bootstrap-into-Unlocked: same Orphan Scan path as the AutoUnlock
		// leg of OpenStore (unlockBootstrap flips state to Unlocked on
		// success). Failure wipes the just-unwrapped DEK and reverts to
		// Locked — holding the key in memory for a non-operational Store
		// adds risk without benefit.
		// The index question is settled here rather than at OpenStore for a
		// Locked store: probing is keyless, but recovery needs the DEK that
		// only now exists (ADR-118).
		if err := settleIndex(ctx, s,
			planIndexOpen(ctx, s.snapshotConfig().ManifestCrypto, s.index, s.drv, s.allowRecovery)); err != nil {
			s.crypto.WipeDEK()
			s.stateMu.Lock()
			s.state = domain.StateLocked
			s.stateMu.Unlock()
			return fmt.Errorf("store.Unlock: %w", err)
		}
		if err := unlockBootstrap(ctx, s, s.pub); err != nil {
			s.crypto.WipeDEK()
			s.stateMu.Lock()
			s.state = domain.StateLocked
			s.stateMu.Unlock()
			return fmt.Errorf("store.Unlock: %w", err)
		}
	default:
		st := s.state
		s.stateMu.Unlock()
		return fmt.Errorf("store.Unlock: state %v rejects unlock", st)
	}

	// Info — an unlock is an operator-relevant access-boundary event. No
	// secret is logged; the DEK never appears.
	s.componentLogger("store").LogAttrs(ctx, slog.LevelInfo, "store unlocked",
		storeIDAttr(s))
	return nil
}

// ExportRecoveryKit returns the current Recovery Kit. It gates on enterRead,
// requires Unlocked or Degraded, and delegates kit assembly to crypto.State.
func (s *store) ExportRecoveryKit(ctx context.Context) ([]byte, error) {
	if err := s.enterRead(ctx); err != nil {
		return nil, err
	}
	switch s.currentState() {
	case domain.StateUnlocked, domain.StateDegraded:
		// OK
	default:
		return nil, fmt.Errorf("store.ExportRecoveryKit: state %v rejects export",
			s.currentState())
	}
	kit, err := s.crypto.RecoveryKit()
	if err != nil {
		return nil, fmt.Errorf("store.ExportRecoveryKit: %w", err)
	}
	return kit, nil
}

// RotateKEK re-wraps the DEK under a new KEK. On-disk data is not rewritten;
// the prior Recovery Kit is invalidated. Same gate and Degraded refusal as
// SetPassphrase; the current-passphrase proof, re-wrap, and persist live in
// crypto.State.
func (s *store) RotateKEK(ctx context.Context) error {
	if err := s.enterWrite(ctx); err != nil {
		return err
	}
	if st := s.currentState(); st == domain.StateDegraded {
		return fmt.Errorf("store.RotateKEK: state %v rejects write; wait for Auto-Heal", st)
	}
	if err := s.crypto.RotateKEK(ctx, s.storeID); err != nil {
		return fmt.Errorf("store.RotateKEK: %w", err)
	}
	// Warn — KEK rotation invalidates the prior Recovery Kit, which the
	// operator must re-export; surfacing it above Info reduces the chance of
	// an orphaned kit. No key material is logged.
	s.componentLogger("store").LogAttrs(ctx, slog.LevelWarn, "KEK rotated; prior recovery kit invalidated",
		storeIDAttr(s))
	return nil
}

// SetPassphrase enables encryption on a Store initialised with a plaintext
// DEK. Refuses with errs.ErrPassphraseAlreadySet when the DEK is already
// wrapped — use RotateKEK then. It gates on enterWrite, refuses in Degraded
// (a descriptor rewrite on already-diverged replicas is unsafe until
// Auto-Heal reaches Unlocked), resolves the KDF cost from config, and
// delegates the wrap+persist to crypto.State.
func (s *store) SetPassphrase(ctx context.Context) error {
	if err := s.enterWrite(ctx); err != nil {
		return err
	}
	if st := s.currentState(); st == domain.StateDegraded {
		return fmt.Errorf("store.SetPassphrase: state %v rejects write; wait for Auto-Heal", st)
	}
	// Cost: from active config if the caller specified KDFParams there;
	// otherwise WrapDEK's default. The descriptor's KDFParams is nil for a
	// Plain Store, so it cannot be the source.
	var cost store2.KDFParams
	if cfg := s.snapshotConfig(); cfg.KDFParams != nil {
		cost = *cfg.KDFParams
	}
	if err := s.crypto.SetPassphrase(ctx, s.storeID, cost); err != nil {
		return fmt.Errorf("store.SetPassphrase: %w", err)
	}
	// Info — the Store transitioned from plaintext-DEK to passphrase-wrapped.
	s.componentLogger("store").LogAttrs(ctx, slog.LevelInfo, "passphrase set; DEK now wrapped",
		storeIDAttr(s))
	return nil
}
