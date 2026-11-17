package replay

import (
	"sync/atomic"
	"testing"

	"github.com/Overclock-Validator/mithril/pkg/accounts"
	"github.com/Overclock-Validator/mithril/pkg/addresses"
	"github.com/Overclock-Validator/mithril/pkg/sealevel"
	"github.com/Overclock-Validator/mithril/pkg/tpu/txfixture"
	"github.com/gagliardetto/solana-go"
)

// A real transaction executed on a sealed context returns exactly what it
// returns on the slot's own context, reads its accounts only through the view
// (which pins each of them), and writes nothing.
func TestWorkAheadSealedExecutionMatchesNormal(t *testing.T) {
	slotCtx, cleanup := newCommitTestSlotCtx()
	defer cleanup()
	overlay := accounts.NewOverlayAccounts(slotCtx.Accounts)
	slotCtx.Accounts = overlay
	tx, err := solana.TransactionFromBytes(txfixture.MustSignedTransferWire(7))
	if err != nil {
		t.Fatal(err)
	}

	normal, panicked, msg := workAheadExecute(LoadAndExecuteTransactionInput{SlotCtx: slotCtx, Transaction: tx, Speculative: true})
	if panicked {
		t.Fatalf("normal execution panicked: %s", msg)
	}
	view := accounts.NewRecordingReader(overlay)
	early, panicked, msg := workAheadExecute(LoadAndExecuteTransactionInput{SlotCtx: sealevel.NewSealedSlotCtx(slotCtx, view), Transaction: tx, Speculative: true})
	if panicked {
		t.Fatalf("sealed execution panicked: %s", msg)
	}
	if normal.ProcessingResult.TransactionError != nil || early.ProcessingResult.TransactionError != nil {
		t.Fatalf("the transfer must succeed: normal %v, sealed %v", normal.ProcessingResult.TransactionError, early.ProcessingResult.TransactionError)
	}
	if diff := compareOutputs(&early, &normal); diff != "" {
		t.Fatalf("sealed execution differs from the normal one: %s", diff)
	}
	if view.Wrote() {
		t.Fatal("an execution must not write through its account view")
	}
	read := map[[32]byte]bool{}
	for _, r := range view.Reads() {
		read[r.Key] = true
	}
	for _, key := range []solana.PublicKey{txfixture.PayerPubkey(), txfixture.DestPubkey()} {
		if !read[[32]byte(key)] {
			t.Fatalf("account %s was read without going through the view", key)
		}
	}
	if len(slotCtx.ModifiedAccts) != 0 || len(slotCtx.WritableAccts) != 0 {
		t.Fatal("a sealed execution must leave the slot's bookkeeping alone")
	}
	if _, stored := overlay.DeltaEntry([32]byte(txfixture.DestPubkey())); stored {
		t.Fatal("a sealed execution must store nothing")
	}
	if !view.Unchanged(nil) {
		t.Fatal("nothing was stored, so every read still holds")
	}
	dest := [32]byte(txfixture.DestPubkey())
	current, _ := overlay.GetAccount(&dest)
	if err := overlay.SetAccount(&dest, current.Clone()); err != nil {
		t.Fatal(err)
	}
	if view.Unchanged(nil) {
		t.Fatal("a store to an account the execution read must be seen")
	}
}

// Adoption requires every account read through the sealed view to be
// unchanged, including one outside the captured read set; an adopted result is
// handed over pointing at the slot's own context.
func TestWorkAheadClaimChecksEveryViewRead(t *testing.T) {
	saved := workAheadValueCheck
	defer func() { workAheadValueCheck = saved }()
	workAheadValueCheck = false
	payer, other, undeclared := testPk(1), testPk(2), testPk(3)
	parent := accounts.NewMemAccountsWithLen(3)
	for _, key := range []solana.PublicKey{payer, other, undeclared} {
		if err := parent.SetAccountWithoutLock(key, &accounts.Account{Key: key, Lamports: 10}); err != nil {
			t.Fatal(err)
		}
	}
	overlay := accounts.NewOverlayAccounts(parent)
	slotCtx := &sealevel.SlotCtx{Accounts: overlay}
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: []solana.PublicKey{payer, other}}}
	w := &workAhead{slotCtx: slotCtx, overlay: overlay, txs: []*solana.Transaction{tx, tx, tx}, entries: make([]atomic.Pointer[workAheadEntry], 3)}
	finishedEntry := func() *workAheadEntry {
		e := &workAheadEntry{state: workAheadFinished, finished: make(chan struct{})}
		close(e.finished)
		e.keys, e.versions, _ = workAheadReadSet(slotCtx, overlay, tx)
		e.view = accounts.NewRecordingReader(overlay)
		sealed := sealevel.NewSealedSlotCtx(slotCtx, e.view)
		for _, key := range []solana.PublicKey{payer, other, undeclared} {
			if _, err := sealed.GetAccountShared(key); err != nil {
				t.Fatal(err)
			}
		}
		e.output.ExecCtx = &sealevel.ExecutionCtx{SlotCtx: sealed}
		return e
	}

	w.entries[0].Store(finishedEntry())
	out, release := w.claim(0)
	if out == nil {
		t.Fatal("nothing read has changed: the result must be adopted")
	}
	if out.ExecCtx.SlotCtx != slotCtx {
		t.Fatal("an adopted result must be committed against the slot's own context, not the sealed one")
	}
	release()

	w.entries[1].Store(finishedEntry())
	key := [32]byte(undeclared)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: undeclared, Lamports: 10}); err != nil {
		t.Fatal(err)
	}
	if out, _ := w.claim(1); out != nil {
		t.Fatal("a store to an account read outside the captured read set must invalidate the result")
	}

	// captured after that store, so only the next one can invalidate it
	w.entries[2].Store(finishedEntry())
	key = [32]byte(other)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: other, Lamports: 10}); err != nil {
		t.Fatal(err)
	}
	if out, _ := w.claim(2); out != nil {
		t.Fatal("without the value check an identical store to a read account still invalidates")
	}
}

// With the value check on, a captured account stored again with identical
// content is still accepted, and the view leaves that decision to it; an
// account outside the captured set is never accepted by content.
func TestWorkAheadViewWithValueCheck(t *testing.T) {
	saved := workAheadValueCheck
	defer func() { workAheadValueCheck = saved }()
	workAheadValueCheck = true
	payer, other, undeclared := testPk(1), testPk(2), testPk(3)
	parent := accounts.NewMemAccountsWithLen(3)
	for _, key := range []solana.PublicKey{payer, other, undeclared} {
		if err := parent.SetAccountWithoutLock(key, &accounts.Account{Key: key, Lamports: 10}); err != nil {
			t.Fatal(err)
		}
	}
	overlay := accounts.NewOverlayAccounts(parent)
	slotCtx := &sealevel.SlotCtx{Accounts: overlay}
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: []solana.PublicKey{payer, other}}}
	w := &workAhead{slotCtx: slotCtx, overlay: overlay}
	e := &workAheadEntry{}
	e.keys, e.versions, e.base = workAheadReadSet(slotCtx, overlay, tx)
	e.view = accounts.NewRecordingReader(overlay)
	for _, key := range []solana.PublicKey{payer, other, undeclared} {
		k := [32]byte(key)
		if _, err := e.view.GetAccount(&k); err != nil {
			t.Fatal(err)
		}
	}
	e.stable = w.versionsUnchanged(e)
	key := [32]byte(other)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: other, Lamports: 10}); err != nil {
		t.Fatal(err)
	}
	if !w.readSetUnchanged(e) {
		t.Fatal("an identical store to a captured account validates with the value check")
	}
	key = [32]byte(undeclared)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: undeclared, Lamports: 10}); err != nil {
		t.Fatal(err)
	}
	if w.readSetUnchanged(e) {
		t.Fatal("an account outside the captured read set is checked by version only")
	}
}

// An execution that tried to write through its sealed view has no usable
// result, whatever it returned.
func TestWorkAheadSealedWriteIsUnusable(t *testing.T) {
	payer := testPk(1)
	parent := accounts.NewMemAccountsWithLen(1)
	if err := parent.SetAccountWithoutLock(payer, &accounts.Account{Key: payer, Lamports: 10}); err != nil {
		t.Fatal(err)
	}
	overlay := accounts.NewOverlayAccounts(parent)
	slotCtx := &sealevel.SlotCtx{Accounts: overlay}
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: []solana.PublicKey{payer}}}
	w := &workAhead{slotCtx: slotCtx, overlay: overlay, txs: []*solana.Transaction{tx}, entries: make([]atomic.Pointer[workAheadEntry], 1)}
	e := &workAheadEntry{state: workAheadRunning, finished: make(chan struct{})}
	e.keys, e.versions, _ = workAheadReadSet(slotCtx, overlay, tx)
	e.view = accounts.NewRecordingReader(overlay)
	sealed := sealevel.NewSealedSlotCtx(slotCtx, e.view)
	if err := sealed.SetAccount(payer, &accounts.Account{Key: payer, Lamports: 11}); err == nil {
		t.Fatal("the sealed context must refuse the write")
	}
	w.entries[0].Store(e)
	w.ran(0, e, LoadAndExecuteTransactionOutput{ExecCtx: &sealevel.ExecutionCtx{SlotCtx: sealed}}, false, true)
	if !e.panicked || w.stats.sealedWrite.Load() != 1 {
		t.Fatal("a write attempt must mark the result unusable")
	}
	if out, _ := w.claim(0); out != nil {
		t.Fatal("a result from an execution that tried to write must never be adopted")
	}
	if got, _ := slotCtx.GetAccountShared(payer); got.Lamports != 10 {
		t.Fatal("the refused write must not reach the slot")
	}
}

// Adversarial case: a program is upgraded (or first deployed) by an earlier
// transaction after a later one already ran early against it. The loader reads
// the program account and then the program data account through the slot's
// accounts, so the sealed view holds both, whether or not the captured read
// set does, and whether the program data existed at all when it was read.
func TestWorkAheadSealedViewCatchesProgramUpgrade(t *testing.T) {
	saved := workAheadValueCheck
	defer func() { workAheadValueCheck = saved }()
	workAheadValueCheck = false
	payer, program, programData, lateData := testPk(1), testPk(2), testPk(3), testPk(4)
	loaderOwner := [32]byte(addresses.BpfLoaderUpgradeableAddr)
	parent := accounts.NewMemAccountsWithLen(3)
	for _, acct := range []*accounts.Account{
		{Key: payer, Lamports: 10},
		{Key: program, Lamports: 10, Owner: loaderOwner, Executable: true},
		{Key: programData, Lamports: 10, Owner: loaderOwner, Data: []byte{1}},
	} {
		if err := parent.SetAccountWithoutLock(acct.Key, acct); err != nil {
			t.Fatal(err)
		}
	}
	overlay := accounts.NewOverlayAccounts(parent)
	slotCtx := &sealevel.SlotCtx{Accounts: overlay}
	// The transaction declares only its payer here, so the captured read set
	// covers neither program account: only the view can catch the upgrade.
	tx := &solana.Transaction{Message: solana.Message{AccountKeys: []solana.PublicKey{payer}}}
	w := &workAhead{slotCtx: slotCtx, overlay: overlay, txs: []*solana.Transaction{tx, tx, tx}, entries: make([]atomic.Pointer[workAheadEntry], 3)}
	earlyRun := func(dataKey solana.PublicKey) *workAheadEntry {
		e := &workAheadEntry{state: workAheadFinished, finished: make(chan struct{})}
		close(e.finished)
		e.keys, e.versions, _ = workAheadReadSet(slotCtx, overlay, tx)
		e.view = accounts.NewRecordingReader(overlay)
		sealed := sealevel.NewSealedSlotCtx(slotCtx, e.view)
		// what the loader does: the program account, then its program data
		if _, err := sealed.GetAccount(program); err != nil {
			t.Fatal(err)
		}
		_, _ = sealed.GetAccountShared(dataKey) // may not exist yet
		e.output.ExecCtx = &sealevel.ExecutionCtx{SlotCtx: sealed}
		return e
	}

	w.entries[0].Store(earlyRun(programData))
	if out, release := w.claim(0); out == nil {
		t.Fatal("no upgrade happened: the early result stands")
	} else {
		release()
	}

	// upgrade: the program data account is stored again
	w.entries[1].Store(earlyRun(programData))
	key := [32]byte(programData)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: programData, Lamports: 10, Owner: loaderOwner, Data: []byte{2}}); err != nil {
		t.Fatal(err)
	}
	if out, _ := w.claim(1); out != nil {
		t.Fatal("an early result survived an upgrade of the program it ran")
	}

	// first deployment: the program data did not exist when the early run
	// looked for it, and is created before the transaction's turn
	w.entries[2].Store(earlyRun(lateData))
	key = [32]byte(lateData)
	if err := overlay.SetAccount(&key, &accounts.Account{Key: lateData, Lamports: 10, Owner: loaderOwner, Data: []byte{9}}); err != nil {
		t.Fatal(err)
	}
	if out, _ := w.claim(2); out != nil {
		t.Fatal("an early result survived the creation of an account it had found missing")
	}
}

// A sealed context reports every direct parent-bank read, and parentReadsUnseen
// counts the ones its view has no record of: reads that rest on the parent not
// changing during the block, without the version check behind them.
func TestWorkAheadParentReadsAreCheckedAgainstTheView(t *testing.T) {
	seenFirst, never := testPk(41), testPk(42)
	parent := accounts.NewMemAccountsWithLen(2)
	for _, key := range []solana.PublicKey{seenFirst, never} {
		if err := parent.SetAccountWithoutLock(key, &accounts.Account{Key: key, Lamports: 7}); err != nil {
			t.Fatal(err)
		}
	}
	base := &sealevel.SlotCtx{Accounts: accounts.NewOverlayAccounts(parent), ParentAccts: parent}
	view := accounts.NewRecordingReader(base.Accounts.(*accounts.OverlayAccounts))
	sealed := sealevel.NewSealedSlotCtx(base, view)
	if sealed.ParentReadHook != nil {
		t.Fatal("a derived context must not inherit a parent read hook")
	}
	var direct [][32]byte
	sealed.ParentReadHook = func(key solana.PublicKey) { direct = append(direct, [32]byte(key)) }

	// The usual shape: ask the slot's accounts (the view), then the parent.
	if _, err := sealed.GetAccountShared(seenFirst); err != nil {
		t.Fatal(err)
	}
	if _, err := sealed.GetParentAccount(seenFirst); err != nil {
		t.Fatal(err)
	}
	if got := parentReadsUnseen(view, direct); len(direct) != 1 || got != 0 {
		t.Fatalf("after a view read then a parent read: %d direct, %d unseen; want 1 and 0", len(direct), got)
	}
	// A parent read the view never saw is counted once, however often it repeats.
	for range 3 {
		if _, err := sealed.GetParentAccount(never); err != nil {
			t.Fatal(err)
		}
	}
	if got := parentReadsUnseen(view, direct); len(direct) != 4 || got != 1 {
		t.Fatalf("after unseen parent reads: %d direct, %d unseen; want 4 and 1", len(direct), got)
	}
	if got := parentReadsUnseen(nil, direct); got != 0 {
		t.Fatalf("no view: %d unseen, want 0", got)
	}
}
