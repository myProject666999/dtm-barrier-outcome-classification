package barrieroutcometest_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dtm-labs/dtm/client/dtmcli"
	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/stretchr/testify/require"
)

const outcomeMemDriverName = "dtm-barrier-outcome-mem"

var (
	outcomeMemDSNSeq   atomic.Uint64
	outcomeMemStores   sync.Map // DSN id -> *outcomeMemStore
	insertOutcomeMemRe = regexp.MustCompile(`(?i)^insert(?:\s+ignore)?\s+into\s+([^\s(]+)\s*\(`)
	selectOutcomeMemRe = regexp.MustCompile(`(?i)^select\s+reason\s+from\s+([^\s]+)\s+where\s+`)

	errOutcomeMemSelect = errors.New("mem driver injected select failure")
	errOutcomeMemCommit = errors.New("mem driver: current transaction is aborted, commands ignored until end of transaction block")
)

func init() {
	sql.Register(outcomeMemDriverName, &outcomeMemDriver{})
}

type outcomeMemKey struct {
	table     string
	gid       string
	branchID  string
	op        string
	barrierID string
}

type outcomeMemRow struct {
	key       outcomeMemKey
	transType string
	reason    string
}

type outcomeMemStmt struct {
	Kind  string
	SQL   string
	Table string
	Args  []driver.Value
}

type outcomeMemTx struct {
	id      uint64
	store   *outcomeMemStore
	stmts   []outcomeMemStmt
	staged  map[outcomeMemKey]outcomeMemRow
	waiters []chan struct{}
	aborted bool
	ended   bool
}

type outcomeMemStore struct {
	mu             sync.Mutex
	mode           string
	nextTxID       uint64
	txs            []*outcomeMemTx
	recorded       []outcomeMemStmt
	committed      map[outcomeMemKey]outcomeMemRow
	active         map[outcomeMemKey]*outcomeMemTx
	failNextSelect bool
	selectErr      error
}

type outcomeMemHarness struct {
	t     *testing.T
	mode  string
	store *outcomeMemStore
	dsn   string
	db    *sql.DB
}

func newOutcomeMemHarness(t *testing.T, mode string) *outcomeMemHarness {
	t.Helper()
	store := &outcomeMemStore{
		mode:      mode,
		committed: map[outcomeMemKey]outcomeMemRow{},
		active:    map[outcomeMemKey]*outcomeMemTx{},
	}
	id := strconv.FormatUint(outcomeMemDSNSeq.Add(1), 10)
	outcomeMemStores.Store(id, store)
	dsn := mode + "|" + id
	db, err := sql.Open(outcomeMemDriverName, dsn)
	require.NoError(t, err)
	db.SetMaxOpenConns(8)

	h := &outcomeMemHarness{t: t, mode: mode, store: store, dsn: dsn, db: db}
	t.Cleanup(func() {
		_ = db.Close()
		outcomeMemStores.Delete(id)
	})
	return h
}

func (h *outcomeMemHarness) barrier(gid, op string, barrierID int) *dtmcli.BranchBarrier {
	return &dtmcli.BranchBarrier{
		TransType: "test",
		Gid:       gid,
		BranchID:  "branch-1",
		Op:        op,
		BarrierID: barrierID,
		DBType:    h.mode,
	}
}

func (h *outcomeMemHarness) begin(t *testing.T) (*sql.Tx, *outcomeMemTx) {
	t.Helper()
	h.store.mu.Lock()
	before := len(h.store.txs)
	h.store.mu.Unlock()

	tx, err := h.db.Begin()
	require.NoError(t, err)

	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	require.Greater(t, len(h.store.txs), before)
	return tx, h.store.txs[len(h.store.txs)-1]
}

func (h *outcomeMemHarness) failNextSelect(err error) {
	if err == nil {
		err = errOutcomeMemSelect
	}
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	h.store.failNextSelect = true
	h.store.selectErr = err
}

func (h *outcomeMemHarness) waitForWaiter(t *testing.T, key outcomeMemKey) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		h.store.mu.Lock()
		holder, ok := h.store.active[key]
		waiting := ok && len(holder.waiters) > 0
		h.store.mu.Unlock()
		if waiting {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for a lock waiter on %+v", key)
}

func (h *outcomeMemHarness) row(key outcomeMemKey) (outcomeMemRow, bool) {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	row, ok := h.store.committed[key]
	return row, ok
}

func (h *outcomeMemHarness) rowsForGID(gid string) []outcomeMemRow {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	var rows []outcomeMemRow
	for _, row := range h.store.committed {
		if row.key.gid == gid {
			rows = append(rows, row)
		}
	}
	return rows
}

func (tx *outcomeMemTx) snapshot() []outcomeMemStmt {
	tx.store.mu.Lock()
	defer tx.store.mu.Unlock()
	return append([]outcomeMemStmt(nil), tx.stmts...)
}

func outcomeMemSQLKinds(stmts []outcomeMemStmt) []string {
	kinds := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		if stmt.Kind == "insert" || stmt.Kind == "select" {
			kinds = append(kinds, stmt.Kind)
		}
	}
	return kinds
}

func outcomeMemAllKinds(stmts []outcomeMemStmt) []string {
	kinds := make([]string, 0, len(stmts))
	for _, stmt := range stmts {
		kinds = append(kinds, stmt.Kind)
	}
	return kinds
}

func outcomeMemKeyFor(bb *dtmcli.BranchBarrier, op, barrierID string) outcomeMemKey {
	table := bb.BarrierTableName
	if table == "" {
		table = dtmimp.BarrierTableName
	}
	return outcomeMemKey{table: table, gid: bb.Gid, branchID: bb.BranchID, op: op, barrierID: barrierID}
}

type outcomeMemDriver struct{}

func (d *outcomeMemDriver) Open(dsn string) (driver.Conn, error) {
	parts := strings.SplitN(dsn, "|", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid outcome memory dsn: %q", dsn)
	}
	value, ok := outcomeMemStores.Load(parts[1])
	if !ok {
		return nil, fmt.Errorf("unknown outcome memory store: %q", parts[1])
	}
	return &outcomeMemConn{store: value.(*outcomeMemStore)}, nil
}

type outcomeMemConn struct {
	store *outcomeMemStore
	tx    *outcomeMemTx
}

func (c *outcomeMemConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("outcome memory driver does not implement prepared statements")
}

func (c *outcomeMemConn) Close() error { return nil }

func (c *outcomeMemConn) Begin() (driver.Tx, error) {
	c.store.mu.Lock()
	defer c.store.mu.Unlock()
	c.store.nextTxID++
	tx := &outcomeMemTx{
		id:     c.store.nextTxID,
		store:  c.store,
		stmts:  []outcomeMemStmt{{Kind: "begin"}},
		staged: map[outcomeMemKey]outcomeMemRow{},
	}
	c.store.txs = append(c.store.txs, tx)
	c.tx = tx
	return tx, nil
}

func (c *outcomeMemConn) BeginTx(context.Context, driver.TxOptions) (driver.Tx, error) {
	return c.Begin()
}

func (c *outcomeMemConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	values := outcomeMemNamedValues(args)
	if insertOutcomeMemRe.MatchString(query) {
		table, row, err := parseOutcomeMemInsert(query, values)
		if err != nil {
			return nil, err
		}
		c.store.record(c.tx, outcomeMemStmt{Kind: "insert", SQL: query, Table: table, Args: append([]driver.Value(nil), values...)})
		return c.store.insert(ctx, c.tx, row)
	}
	return nil, fmt.Errorf("unexpected exec SQL: %s", query)
}

func (c *outcomeMemConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	values := outcomeMemNamedValues(args)
	if !selectOutcomeMemRe.MatchString(query) {
		return nil, fmt.Errorf("unexpected query SQL: %s", query)
	}
	table, key, err := parseOutcomeMemSelect(query, values)
	if err != nil {
		return nil, err
	}
	c.store.record(c.tx, outcomeMemStmt{Kind: "select", SQL: query, Table: table, Args: append([]driver.Value(nil), values...)})
	return c.store.queryReason(c.tx, key)
}

func outcomeMemNamedValues(args []driver.NamedValue) []driver.Value {
	values := make([]driver.Value, len(args))
	for i, arg := range args {
		values[i] = arg.Value
	}
	return values
}

func parseOutcomeMemInsert(query string, args []driver.Value) (string, outcomeMemRow, error) {
	matches := insertOutcomeMemRe.FindStringSubmatch(query)
	if matches == nil {
		return "", outcomeMemRow{}, fmt.Errorf("unrecognized insert SQL: %s", query)
	}
	if len(args) != 6 {
		return "", outcomeMemRow{}, fmt.Errorf("insert wants 6 args, got %d", len(args))
	}
	row := outcomeMemRow{
		transType: outcomeMemString(args[0]),
		key: outcomeMemKey{
			table:     matches[1],
			gid:       outcomeMemString(args[1]),
			branchID:  outcomeMemString(args[2]),
			op:        outcomeMemString(args[3]),
			barrierID: outcomeMemString(args[4]),
		},
		reason: outcomeMemString(args[5]),
	}
	return matches[1], row, nil
}

func parseOutcomeMemSelect(query string, args []driver.Value) (string, outcomeMemKey, error) {
	matches := selectOutcomeMemRe.FindStringSubmatch(query)
	if matches == nil {
		return "", outcomeMemKey{}, fmt.Errorf("unrecognized select SQL: %s", query)
	}
	if len(args) != 4 {
		return "", outcomeMemKey{}, fmt.Errorf("select wants 4 args, got %d", len(args))
	}
	key := outcomeMemKey{
		table:     matches[1],
		gid:       outcomeMemString(args[0]),
		branchID:  outcomeMemString(args[1]),
		op:        outcomeMemString(args[2]),
		barrierID: outcomeMemString(args[3]),
	}
	return matches[1], key, nil
}

func outcomeMemString(value driver.Value) string {
	switch v := value.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	default:
		return fmt.Sprint(v)
	}
}

func (s *outcomeMemStore) record(tx *outcomeMemTx, stmt outcomeMemStmt) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recorded = append(s.recorded, stmt)
	if tx != nil {
		tx.stmts = append(tx.stmts, stmt)
	}
}

func (h *outcomeMemHarness) recordedStatements() []outcomeMemStmt {
	h.store.mu.Lock()
	defer h.store.mu.Unlock()
	return append([]outcomeMemStmt(nil), h.store.recorded...)
}

type outcomeMemResult struct{ affected int64 }

func (r outcomeMemResult) LastInsertId() (int64, error) { return 0, nil }
func (r outcomeMemResult) RowsAffected() (int64, error) { return r.affected, nil }

func (s *outcomeMemStore) insert(ctx context.Context, tx *outcomeMemTx, row outcomeMemRow) (driver.Result, error) {
	for {
		s.mu.Lock()
		if tx != nil {
			if _, ok := tx.staged[row.key]; ok {
				s.mu.Unlock()
				return outcomeMemResult{affected: 0}, nil
			}
		}
		if _, ok := s.committed[row.key]; ok {
			s.mu.Unlock()
			return outcomeMemResult{affected: 0}, nil
		}
		holder, blocked := s.active[row.key]
		if blocked && holder != tx {
			wait := make(chan struct{})
			holder.waiters = append(holder.waiters, wait)
			s.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				s.removeWaiter(holder, wait)
				return nil, ctx.Err()
			}
		}

		if tx != nil {
			tx.staged[row.key] = row
			s.active[row.key] = tx
		} else {
			s.committed[row.key] = row
		}
		s.mu.Unlock()
		return outcomeMemResult{affected: 1}, nil
	}
}

func (s *outcomeMemStore) removeWaiter(tx *outcomeMemTx, wait chan struct{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, ch := range tx.waiters {
		if ch == wait {
			tx.waiters = append(tx.waiters[:i], tx.waiters[i+1:]...)
			return
		}
	}
}

func (s *outcomeMemStore) queryReason(tx *outcomeMemTx, key outcomeMemKey) (driver.Rows, error) {
	s.mu.Lock()
	if s.failNextSelect {
		s.failNextSelect = false
		if s.mode == dtmimp.DBTypePostgres && tx != nil {
			tx.aborted = true
		}
		err := s.selectErr
		s.mu.Unlock()
		return nil, err
	}
	var reason string
	found := false
	if tx != nil {
		var row outcomeMemRow
		row, found = tx.staged[key]
		if found {
			reason = row.reason
		}
	}
	if !found {
		if row, ok := s.committed[key]; ok {
			reason, found = row.reason, true
		}
	}
	s.mu.Unlock()
	return &outcomeMemRows{reason: reason, has: found}, nil
}

func (s *outcomeMemStore) commit(tx *outcomeMemTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tx.ended {
		return nil
	}
	tx.stmts = append(tx.stmts, outcomeMemStmt{Kind: "commit"})
	if s.mode == dtmimp.DBTypePostgres && tx.aborted {
		tx.finishLocked(s)
		return errOutcomeMemCommit
	}
	for key, row := range tx.staged {
		s.committed[key] = row
	}
	tx.finishLocked(s)
	return nil
}

func (s *outcomeMemStore) rollback(tx *outcomeMemTx) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if tx.ended {
		return nil
	}
	tx.stmts = append(tx.stmts, outcomeMemStmt{Kind: "rollback"})
	tx.finishLocked(s)
	return nil
}

func (tx *outcomeMemTx) finishLocked(s *outcomeMemStore) {
	tx.ended = true
	for key := range tx.staged {
		if holder, ok := s.active[key]; ok && holder == tx {
			delete(s.active, key)
		}
	}
	for _, wait := range tx.waiters {
		close(wait)
	}
	tx.waiters = nil
}

func (tx *outcomeMemTx) Commit() error   { return tx.store.commit(tx) }
func (tx *outcomeMemTx) Rollback() error { return tx.store.rollback(tx) }

type outcomeMemRows struct {
	reason string
	has    bool
	sent   bool
}

func (r *outcomeMemRows) Columns() []string { return []string{"reason"} }
func (r *outcomeMemRows) Close() error      { return nil }

func (r *outcomeMemRows) Next(dest []driver.Value) error {
	if !r.has || r.sent {
		return io.EOF
	}
	dest[0] = r.reason
	r.sent = true
	return nil
}

type outcomeCallResult struct {
	outcome dtmcli.BarrierOutcome
	err     error
}

func recOutcomeCall(bb *dtmcli.BranchBarrier, tx *sql.Tx, calls *atomic.Int32, errBusi error) outcomeCallResult {
	outcome, err := bb.CallWithOutcome(tx, func(*sql.Tx) error {
		calls.Add(1)
		return errBusi
	})
	return outcomeCallResult{outcome: outcome, err: err}
}

func requireOutcomeCall(t *testing.T, ch <-chan outcomeCallResult, want dtmcli.BarrierOutcome) outcomeCallResult {
	t.Helper()
	select {
	case res := <-ch:
		require.Equal(t, want, res.outcome)
		return res
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for outcome %s", want)
		return outcomeCallResult{}
	}
}

func TestBarrierOutcomeOrderTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ops  []string
		want []dtmcli.BarrierOutcome
	}{
		{
			name: "cancel before try",
			ops:  []string{dtmimp.OpCancel, dtmimp.OpTry},
			want: []dtmcli.BarrierOutcome{dtmcli.EmptyCompensation, dtmcli.Suspended},
		},
		{
			name: "try cancel retry try",
			ops:  []string{dtmimp.OpTry, dtmimp.OpCancel, dtmimp.OpTry},
			want: []dtmcli.BarrierOutcome{dtmcli.Executed, dtmcli.Executed, dtmcli.Duplicate},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			h := newOutcomeMemHarness(t, dtmimp.DBTypeMysql)
			var calls atomic.Int32
			for i, op := range tt.ops {
				bb := h.barrier("order-"+tt.name, op, 0)
				tx, _ := h.begin(t)
				res := recOutcomeCall(bb, tx, &calls, nil)
				require.NoError(t, res.err)
				require.Equal(t, tt.want[i], res.outcome)
				require.Equal(t, tt.want[i].String(), res.outcome.String())
			}

			if tt.name == "cancel before try" {
				require.Equal(t, int32(0), calls.Load())
			} else {
				require.Equal(t, int32(2), calls.Load())
			}
		})
	}
}

func TestBarrierOutcomeCallMatchesBusinessCalls(t *testing.T) {
	t.Parallel()

	for _, ops := range [][]string{
		{dtmimp.OpCancel, dtmimp.OpTry},
		{dtmimp.OpTry, dtmimp.OpCancel, dtmimp.OpTry},
	} {
		h := newOutcomeMemHarness(t, dtmimp.DBTypeMysql)
		var calls atomic.Int32
		for _, op := range ops {
			bb := h.barrier("call-compat", op, 0)
			tx, _ := h.begin(t)
			err := bb.Call(tx, func(*sql.Tx) error {
				calls.Add(1)
				return nil
			})
			require.NoError(t, err)
		}
		switch len(ops) {
		case 2:
			require.Equal(t, int32(0), calls.Load())
		case 3:
			require.Equal(t, int32(2), calls.Load())
		}
	}
}

func TestBarrierOutcomeMessageReasons(t *testing.T) {
	t.Parallel()

	t.Run("query prepared rollback suspends msg", func(t *testing.T) {
		h := newOutcomeMemHarness(t, dtmimp.DBTypeMysql)
		prepared := h.barrier("msg-gid", dtmimp.MsgDoOp, 0)
		err := prepared.QueryPrepared(h.db)
		require.ErrorIs(t, err, dtmcli.ErrFailure)

		bb := h.barrier("msg-gid", dtmimp.MsgDoOp, 0)
		bb.BranchID = dtmimp.MsgDoBranch0
		var calls atomic.Int32
		tx, _ := h.begin(t)
		res := recOutcomeCall(bb, tx, &calls, nil)
		require.ErrorIs(t, res.err, dtmcli.ErrDuplicated)
		require.Equal(t, dtmcli.Suspended, res.outcome)
		require.Equal(t, int32(0), calls.Load())

		callBB := h.barrier("msg-gid", dtmimp.MsgDoOp, 0)
		callBB.BranchID = dtmimp.MsgDoBranch0
		tx2, _ := h.begin(t)
		err = callBB.Call(tx2, func(*sql.Tx) error {
			t.Fatal("business call must not run")
			return nil
		})
		require.ErrorIs(t, err, dtmcli.ErrDuplicated)
	})

	t.Run("own msg row is duplicate", func(t *testing.T) {
		h := newOutcomeMemHarness(t, dtmimp.DBTypeMysql)
		bb := h.barrier("msg-own", dtmimp.MsgDoOp, 0)
		bb.BranchID = dtmimp.MsgDoBranch0
		var calls atomic.Int32

		tx, _ := h.begin(t)
		res := recOutcomeCall(bb, tx, &calls, nil)
		require.NoError(t, res.err)
		require.Equal(t, dtmcli.Executed, res.outcome)

		second := h.barrier("msg-own", dtmimp.MsgDoOp, 0)
		second.BranchID = dtmimp.MsgDoBranch0
		tx2, _ := h.begin(t)
		res = recOutcomeCall(second, tx2, &calls, nil)
		require.ErrorIs(t, res.err, dtmcli.ErrDuplicated)
		require.Equal(t, dtmcli.Duplicate, res.outcome)
		require.Equal(t, int32(1), calls.Load())
	})
}

func TestBarrierOutcomeInterleavedTryFailureCancelWait(t *testing.T) {
	t.Parallel()

	h := newOutcomeMemHarness(t, dtmimp.DBTypeMysql)
	gid := "interleaved"
	var calls atomic.Int32

	tryBB := h.barrier(gid, dtmimp.OpTry, 1) // newBarrierID turns preset 1 into 02
	tryTx, tryMemTx := h.begin(t)

	cancelResultCh := make(chan outcomeCallResult, 1)
	tryErr := tryBB.Call(tryTx, func(*sql.Tx) error {
		if calls.Add(1) != 1 {
			t.Errorf("try business call count = %d, want 1", calls.Load())
		}

		cancelBB := h.barrier(gid, dtmimp.OpCancel, 1)
		cancelTx, _ := h.begin(t)
		go func() {
			cancelResultCh <- recOutcomeCall(cancelBB, cancelTx, &calls, nil)
		}()

		blockedKey := outcomeMemKeyFor(cancelBB, dtmimp.OpTry, "02")
		h.waitForWaiter(t, blockedKey)
		return errors.New("try business failed and rolls back")
	})
	require.ErrorContains(t, tryErr, "try business failed")

	cancelRes := requireOutcomeCall(t, cancelResultCh, dtmcli.EmptyCompensation)
	require.NoError(t, cancelRes.err)

	retryBB := h.barrier(gid, dtmimp.OpTry, 1)
	retryTx, retryMemTx := h.begin(t)
	retryRes := recOutcomeCall(retryBB, retryTx, &calls, nil)
	require.NoError(t, retryRes.err)
	require.Equal(t, dtmcli.Suspended, retryRes.outcome)
	require.Equal(t, int32(1), calls.Load())

	rows := h.rowsForGID(gid)
	require.Len(t, rows, 2)
	reasons := map[string]string{}
	for _, row := range rows {
		reasons[row.key.op] = row.reason
	}
	require.Equal(t, map[string]string{
		dtmimp.OpTry:    dtmimp.OpCancel,
		dtmimp.OpCancel: dtmimp.OpCancel,
	}, reasons)

	require.Equal(t, []string{"insert"}, outcomeMemSQLKinds(tryMemTx.snapshot()))
	require.Equal(t, []string{"begin", "insert", "rollback"}, outcomeMemAllKinds(tryMemTx.snapshot()))
	require.Equal(t, []string{"insert", "insert"}, outcomeMemSQLKinds(h.store.txs[1].snapshot()))
	require.Equal(t, []string{"insert", "select"}, outcomeMemSQLKinds(retryMemTx.snapshot()))
}

func TestOutcomeMemoryDriverUniqueLock(t *testing.T) {
	t.Parallel()

	for _, commitFirst := range []bool{true, false} {
		commitFirst := commitFirst
		t.Run(fmt.Sprintf("commit=%v", commitFirst), func(t *testing.T) {
			h := newOutcomeMemHarness(t, dtmimp.DBTypeMysql)
			gid := "lock-" + strconv.FormatBool(commitFirst)
			holderBB := h.barrier(gid, dtmimp.OpTry, 0)
			holderTx, _ := h.begin(t)
			var calls atomic.Int32

			waiterTx, _ := h.begin(t)
			affectedCh := make(chan int64, 1)
			errCh := make(chan error, 1)

			holderErr := holderBB.Call(holderTx, func(*sql.Tx) error {
				go func() {
					n, err := dtmimp.InsertBarrier(waiterTx, "test", gid, "branch-1", dtmimp.OpTry, "01", dtmimp.OpTry, h.mode, "")
					affectedCh <- n
					errCh <- err
				}()
				key := outcomeMemKeyFor(holderBB, dtmimp.OpTry, "01")
				h.waitForWaiter(t, key)
				if commitFirst {
					calls.Add(1)
					return nil
				}
				return errors.New("rollback holder")
			})

			if commitFirst {
				require.NoError(t, holderErr)
				require.Equal(t, int32(1), calls.Load())
			} else {
				require.ErrorContains(t, holderErr, "rollback holder")
			}

			select {
			case affected := <-affectedCh:
				if commitFirst {
					require.Equal(t, int64(0), affected)
				} else {
					require.Equal(t, int64(1), affected)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for blocked insert")
			}
			select {
			case err := <-errCh:
				require.NoError(t, err)
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for blocked insert error")
			}
			require.NoError(t, waiterTx.Rollback())
		})
	}
}

func TestBarrierOutcomeBarrierID02AndStatementCounts(t *testing.T) {
	t.Parallel()

	h := newOutcomeMemHarness(t, dtmimp.DBTypeMysql)
	gid := "barrier-id-02"

	cancel := h.barrier(gid, dtmimp.OpCancel, 1)
	cancelTx, cancelMemTx := h.begin(t)
	res := recOutcomeCall(cancel, cancelTx, &atomic.Int32{}, nil)
	require.NoError(t, res.err)
	require.Equal(t, dtmcli.EmptyCompensation, res.outcome)
	require.Equal(t, []string{"insert", "insert"}, outcomeMemSQLKinds(cancelMemTx.snapshot()))
	for _, stmt := range cancelMemTx.snapshot() {
		if stmt.Kind == "insert" {
			require.Equal(t, "02", stmt.Args[4])
		}
	}

	try := h.barrier(gid, dtmimp.OpTry, 1)
	tryTx, tryMemTx := h.begin(t)
	res = recOutcomeCall(try, tryTx, &atomic.Int32{}, nil)
	require.NoError(t, res.err)
	require.Equal(t, dtmcli.Suspended, res.outcome)
	stmts := tryMemTx.snapshot()
	require.Equal(t, []string{"insert", "select"}, outcomeMemSQLKinds(stmts))
	var selectStmt outcomeMemStmt
	for _, stmt := range stmts {
		switch stmt.Kind {
		case "insert":
			require.Equal(t, "02", stmt.Args[4])
		case "select":
			selectStmt = stmt
			require.Equal(t, "02", stmt.Args[3])
		}
	}
	require.Equal(t, dtmimp.BarrierTableName, selectStmt.Table)

	duplicateCancel := h.barrier(gid, dtmimp.OpCancel, 1)
	dupTx, dupMemTx := h.begin(t)
	res = recOutcomeCall(duplicateCancel, dupTx, &atomic.Int32{}, nil)
	require.NoError(t, res.err)
	require.Equal(t, dtmcli.Duplicate, res.outcome)
	require.Equal(t, []string{"insert", "insert"}, outcomeMemSQLKinds(dupMemTx.snapshot()))
}

func TestBarrierOutcomePostgresLookupError(t *testing.T) {
	t.Parallel()

	for _, useCall := range []bool{false, true} {
		useCall := useCall
		t.Run(fmt.Sprintf("use Call=%v", useCall), func(t *testing.T) {
			h := newOutcomeMemHarness(t, dtmimp.DBTypePostgres)
			gid := "pg-lookup-error"

			cancel := h.barrier(gid, dtmimp.OpCancel, 1)
			cancelTx, _ := h.begin(t)
			res := recOutcomeCall(cancel, cancelTx, &atomic.Int32{}, nil)
			require.NoError(t, res.err)
			require.Equal(t, dtmcli.EmptyCompensation, res.outcome)

			h.failNextSelect(errOutcomeMemSelect)
			try := h.barrier(gid, dtmimp.OpTry, 1)
			tryTx, tryMemTx := h.begin(t)
			var calls atomic.Int32

			if useCall {
				err := try.Call(tryTx, func(*sql.Tx) error {
					calls.Add(1)
					return nil
				})
				require.NoError(t, err)
			} else {
				res := recOutcomeCall(try, tryTx, &calls, nil)
				require.Equal(t, dtmcli.Undetermined, res.outcome)
				require.Same(t, errOutcomeMemSelect, res.err)
			}
			require.Equal(t, int32(0), calls.Load())

			stmts := tryMemTx.snapshot()
			require.Equal(t, []string{"begin", "insert", "select", "rollback"}, outcomeMemAllKinds(stmts))
			require.Contains(t, stmts[2].SQL, "$4")

			rows := h.rowsForGID(gid)
			require.Len(t, rows, 2)
			for _, row := range rows {
				require.Equal(t, dtmimp.OpCancel, row.reason)
			}
		})
	}
}

func TestOutcomeMemoryPostgresAbortedCommit(t *testing.T) {
	t.Parallel()

	h := newOutcomeMemHarness(t, dtmimp.DBTypePostgres)
	tx, _ := h.begin(t)
	h.failNextSelect(errOutcomeMemSelect)

	var reason string
	err := tx.QueryRow(
		"select reason from dtm_barrier.barrier where gid=$1 and branch_id=$2 and op=$3 and barrier_id=$4",
		"gid", "branch", "op", "01",
	).Scan(&reason)
	require.ErrorIs(t, err, errOutcomeMemSelect)
	err = tx.Commit()
	require.ErrorIs(t, err, errOutcomeMemCommit)
}

func TestBarrierOutcomeCustomTableName(t *testing.T) {
	t.Parallel()

	h := newOutcomeMemHarness(t, dtmimp.DBTypeMysql)
	gid := "custom-table"
	customTable := "custom_barrier_table"

	cancel := h.barrier(gid, dtmimp.OpCancel, 0)
	cancel.BarrierTableName = customTable
	cancelTx, cancelMemTx := h.begin(t)
	res := recOutcomeCall(cancel, cancelTx, &atomic.Int32{}, nil)
	require.NoError(t, res.err)
	require.Equal(t, dtmcli.EmptyCompensation, res.outcome)

	try := h.barrier(gid, dtmimp.OpTry, 0)
	try.BarrierTableName = customTable
	tryTx, tryMemTx := h.begin(t)
	res = recOutcomeCall(try, tryTx, &atomic.Int32{}, nil)
	require.NoError(t, res.err)
	require.Equal(t, dtmcli.Suspended, res.outcome)

	for _, mtx := range []*outcomeMemTx{cancelMemTx, tryMemTx} {
		for _, stmt := range mtx.snapshot() {
			switch stmt.Kind {
			case "insert", "select":
				require.Equal(t, customTable, stmt.Table)
			}
		}
	}
	key := outcomeMemKeyFor(try, dtmimp.OpTry, "01")
	row, ok := h.row(key)
	require.True(t, ok)
	require.Equal(t, customTable, row.key.table)
	require.Equal(t, dtmimp.OpCancel, row.reason)
}

func TestBarrierQueryPreparedCustomTableName(t *testing.T) {
	t.Parallel()

	h := newOutcomeMemHarness(t, dtmimp.DBTypeMysql)
	bb := h.barrier("custom-query-prepared", dtmimp.MsgDoOp, 0)
	bb.BarrierTableName = "custom_prepared_table"

	err := bb.QueryPrepared(h.db)
	require.ErrorIs(t, err, dtmcli.ErrFailure)

	var insertTables, selectTables []string
	for _, stmt := range h.recordedStatements() {
		switch stmt.Kind {
		case "insert":
			insertTables = append(insertTables, stmt.Table)
		case "select":
			selectTables = append(selectTables, stmt.Table)
		}
	}
	require.Equal(t, []string{"custom_prepared_table"}, insertTables)
	require.Equal(t, []string{"custom_prepared_table"}, selectTables)
}

func TestBarrierCallPanicRollsBackAndRethrows(t *testing.T) {
	t.Parallel()

	h := newOutcomeMemHarness(t, dtmimp.DBTypeMysql)
	bb := h.barrier("panic", dtmimp.OpAction, 0)
	tx, memTx := h.begin(t)

	require.PanicsWithValue(t, "boom", func() {
		_ = bb.Call(tx, func(*sql.Tx) error {
			panic("boom")
		})
	})
	require.Equal(t, []string{"begin", "insert", "rollback"}, outcomeMemAllKinds(memTx.snapshot()))

	var err error
	func() {
		defer dtmimp.P2E(&err)
		tx2, _ := h.begin(t)
		bb2 := h.barrier("panic-p2e", dtmimp.OpAction, 0)
		_ = bb2.Call(tx2, func(*sql.Tx) error {
			panic("wrapped boom")
		})
	}()
	require.ErrorContains(t, err, "wrapped boom")
}

func TestBarrierOutcomeSequentialBarrierIDs(t *testing.T) {
	t.Parallel()

	h := newOutcomeMemHarness(t, dtmimp.DBTypeMysql)
	bb := h.barrier("sequential-ids", dtmimp.OpAction, 0)
	var calls atomic.Int32

	for _, wantID := range []string{"01", "02"} {
		tx, memTx := h.begin(t)
		res := recOutcomeCall(bb, tx, &calls, nil)
		require.NoError(t, res.err)
		require.Equal(t, dtmcli.Executed, res.outcome)
		stmts := memTx.snapshot()
		require.Len(t, outcomeMemSQLKinds(stmts), 1)
		require.Equal(t, wantID, stmts[1].Args[4]) // stmts[0] is begin, stmts[1] is insert
	}
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, 2, bb.BarrierID)
}

func TestBarrierOutcomeStringTokens(t *testing.T) {
	t.Parallel()

	require.Equal(t, "executed", dtmcli.BarrierOutcomeExecuted.String())
	require.Equal(t, "empty_compensation", dtmcli.BarrierOutcomeEmptyCompensation.String())
	require.Equal(t, "duplicate", dtmcli.BarrierOutcomeDuplicate.String())
	require.Equal(t, "suspended", dtmcli.BarrierOutcomeSuspended.String())
	require.Equal(t, "undetermined", dtmcli.BarrierOutcomeUndetermined.String())
	require.Equal(t, "unknown", dtmcli.BarrierOutcomeUnknown.String())
}
