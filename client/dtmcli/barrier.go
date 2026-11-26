/*
 * Copyright (c) 2021 yedf. All rights reserved.
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */

package dtmcli

import (
	"database/sql"
	"fmt"
	"net/url"

	"github.com/dtm-labs/dtm/client/dtmcli/dtmimp"
	"github.com/dtm-labs/logger"
)

// BarrierBusiFunc type for busi func
type BarrierBusiFunc func(tx *sql.Tx) error

// BarrierOutcome describes why a barrier call did or did not invoke its
// business function.
type BarrierOutcome string

const (
	// BarrierOutcomeUnknown is used when no stable classification is available.
	BarrierOutcomeUnknown BarrierOutcome = "unknown"
	// BarrierOutcomeExecuted means busiCall was invoked.
	BarrierOutcomeExecuted BarrierOutcome = "executed"
	// BarrierOutcomeEmptyCompensation means the compensation arrived before its origin operation.
	BarrierOutcomeEmptyCompensation BarrierOutcome = "empty_compensation"
	// BarrierOutcomeDuplicate means this exact branch operation already exists.
	BarrierOutcomeDuplicate BarrierOutcome = "duplicate"
	// BarrierOutcomeSuspended means a later operation was blocked by an earlier compensation record.
	BarrierOutcomeSuspended BarrierOutcome = "suspended"
	// BarrierOutcomeUndetermined means the duplicate row exists, but its reason could not be read.
	BarrierOutcomeUndetermined BarrierOutcome = "undetermined"
)

// Short aliases for callers that prefer the names used in barrier discussions.
const (
	// Unknown is the short alias for BarrierOutcomeUnknown.
	Unknown = BarrierOutcomeUnknown
	// Executed is the short alias for BarrierOutcomeExecuted.
	Executed = BarrierOutcomeExecuted
	// EmptyCompensation is the short alias for BarrierOutcomeEmptyCompensation.
	EmptyCompensation = BarrierOutcomeEmptyCompensation
	// Duplicate is the short alias for BarrierOutcomeDuplicate.
	Duplicate = BarrierOutcomeDuplicate
	// Suspended is the short alias for BarrierOutcomeSuspended.
	Suspended = BarrierOutcomeSuspended
	// Undetermined is the short alias for BarrierOutcomeUndetermined.
	Undetermined = BarrierOutcomeUndetermined
)

// String returns the stable lower-snake-case token for an outcome.
func (o BarrierOutcome) String() string {
	switch o {
	case BarrierOutcomeExecuted:
		return "executed"
	case BarrierOutcomeEmptyCompensation:
		return "empty_compensation"
	case BarrierOutcomeDuplicate:
		return "duplicate"
	case BarrierOutcomeSuspended:
		return "suspended"
	case BarrierOutcomeUndetermined:
		return "undetermined"
	default:
		return "unknown"
	}
}

// BranchBarrier every branch info
type BranchBarrier struct {
	TransType        string
	Gid              string
	BranchID         string
	Op               string
	BarrierID        int
	DBType           string // DBTypeMysql | DBTypePostgres
	BarrierTableName string
}

func (bb *BranchBarrier) String() string {
	return fmt.Sprintf("transInfo: %s %s %s %s", bb.TransType, bb.Gid, bb.BranchID, bb.Op)
}

func (bb *BranchBarrier) newBarrierID() string {
	bb.BarrierID++
	return fmt.Sprintf("%02d", bb.BarrierID)
}

// BarrierFromQuery construct transaction info from request
func BarrierFromQuery(qs url.Values) (*BranchBarrier, error) {
	return BarrierFrom(dtmimp.EscapeGet(qs, "trans_type"), dtmimp.EscapeGet(qs, "gid"), dtmimp.EscapeGet(qs, "branch_id"), dtmimp.EscapeGet(qs, "op"))
}

// BarrierFrom construct transaction info from request
func BarrierFrom(transType, gid, branchID, op string) (*BranchBarrier, error) {
	ti := &BranchBarrier{
		TransType: transType,
		Gid:       gid,
		BranchID:  branchID,
		Op:        op,
	}
	if ti.TransType == "" || ti.Gid == "" || ti.BranchID == "" || ti.Op == "" {
		return nil, fmt.Errorf("invalid trans info: %v", ti)
	}
	return ti, nil
}

// Call see detail description in https://en.dtm.pub/practice/barrier.html
// tx: local transaction connection
// busiCall: busi func
func (bb *BranchBarrier) Call(tx *sql.Tx, busiCall BarrierBusiFunc) error {
	outcome, err := bb.CallWithOutcome(tx, busiCall)
	// Call has never propagated the diagnostic reason-lookup failure. The
	// outcome method has already rolled that transaction back. Msg duplicate
	// detection keeps its historical sentinel return.
	if outcome == BarrierOutcomeUndetermined {
		if bb.Op == dtmimp.MsgDoOp {
			return ErrDuplicated
		}
		return nil
	}
	return err
}

// CallWithOutcome behaves like Call and additionally returns the barrier
// classification. The returned error still contains business, insert, commit
// and msg-duplicate errors. For BarrierOutcomeUndetermined, the error is the
// reason lookup error; Call intentionally suppresses that new diagnostic error
// to preserve its historical return value.
func (bb *BranchBarrier) CallWithOutcome(tx *sql.Tx, busiCall BarrierBusiFunc) (outcome BarrierOutcome, rerr error) {
	outcome = BarrierOutcomeUnknown
	bid := bb.newBarrierID()
	defer dtmimp.DeferDo(&rerr, func() error {
		return tx.Commit()
	}, func() error {
		return tx.Rollback()
	})
	originOp := map[string]string{
		dtmimp.OpCancel:     dtmimp.OpTry,    // tcc
		dtmimp.OpCompensate: dtmimp.OpAction, // saga
		dtmimp.OpRollback:   dtmimp.OpAction, // workflow
	}[bb.Op]

	originAffected, oerr := dtmimp.InsertBarrier(tx, bb.TransType, bb.Gid, bb.BranchID, originOp, bid, bb.Op, bb.DBType, bb.BarrierTableName)
	currentAffected, rerr := dtmimp.InsertBarrier(tx, bb.TransType, bb.Gid, bb.BranchID, bb.Op, bid, bb.Op, bb.DBType, bb.BarrierTableName)
	logger.Debugf("originAffected: %d currentAffected: %d", originAffected, currentAffected)

	compensating := bb.Op == dtmimp.OpCancel || bb.Op == dtmimp.OpCompensate || bb.Op == dtmimp.OpRollback
	if rerr == nil && currentAffected == 0 && !compensating {
		reason, qerr := bb.queryBarrierReason(tx, bid)
		if qerr != nil {
			outcome = BarrierOutcomeUndetermined
			rerr = qerr
			bb.logOutcome(bid, outcome, "", qerr)
			return outcome, rerr
		}
		if reason == bb.Op {
			outcome = BarrierOutcomeDuplicate
		} else {
			outcome = BarrierOutcomeSuspended
		}
		bb.logOutcome(bid, outcome, reason, nil)
	}

	if rerr == nil && bb.Op == dtmimp.MsgDoOp && currentAffected == 0 { // for msg's DoAndSubmit, repeated insert should be rejected.
		rerr = ErrDuplicated
		return outcome, rerr
	}

	if rerr == nil {
		rerr = oerr
	}

	if (compensating && originAffected > 0) || // null compensate
		currentAffected == 0 { // repeated request or dangled request
		if rerr == nil {
			if compensating && originAffected > 0 {
				outcome = BarrierOutcomeEmptyCompensation
			} else if outcome == BarrierOutcomeUnknown {
				outcome = BarrierOutcomeDuplicate
			}
			bb.logOutcome(bid, outcome, bb.Op, nil)
		}
		return outcome, rerr
	}
	if rerr == nil {
		outcome = BarrierOutcomeExecuted
		bb.logOutcome(bid, outcome, bb.Op, nil)
		rerr = busiCall(tx)
	}
	return outcome, rerr
}

func (bb *BranchBarrier) barrierTableName() string {
	return dtmimp.OrString(bb.BarrierTableName, dtmimp.BarrierTableName)
}

func (bb *BranchBarrier) queryBarrierReason(q dtmimp.DB, bid string) (string, error) {
	tableName := bb.barrierTableName()
	query := fmt.Sprintf("select reason from %s where gid=? and branch_id=? and op=? and barrier_id=?", tableName)
	query = dtmimp.GetDBSpecial(bb.DBType).GetPlaceHoldSQL(query)
	var reason string
	err := q.QueryRow(query, bb.Gid, bb.BranchID, bb.Op, bid).Scan(&reason)
	return reason, err
}

func (bb *BranchBarrier) logOutcome(bid string, outcome BarrierOutcome, reason string, detail error) {
	if outcome == BarrierOutcomeExecuted {
		logger.Debugf("barrier outcome gid=%s branch_id=%s op=%s barrier_id=%s result=%s reason=%s",
			bb.Gid, bb.BranchID, bb.Op, bid, outcome.String(), reason)
		return
	}
	if detail != nil {
		logger.Infof("barrier outcome gid=%s branch_id=%s op=%s barrier_id=%s result=%s reason=%s err=%v",
			bb.Gid, bb.BranchID, bb.Op, bid, outcome.String(), reason, detail)
		return
	}
	logger.Infof("barrier outcome gid=%s branch_id=%s op=%s barrier_id=%s result=%s reason=%s",
		bb.Gid, bb.BranchID, bb.Op, bid, outcome.String(), reason)
}

// CallWithDB the same as Call, but with *sql.DB
func (bb *BranchBarrier) CallWithDB(db *sql.DB, busiCall BarrierBusiFunc) error {
	tx, err := db.Begin()
	if err == nil {
		err = bb.Call(tx, busiCall)
	}
	return err
}

// QueryPrepared queries prepared data
func (bb *BranchBarrier) QueryPrepared(db *sql.DB) error {
	_, err := dtmimp.InsertBarrier(db, bb.TransType, bb.Gid, dtmimp.MsgDoBranch0, dtmimp.MsgDoOp, dtmimp.MsgDoBarrier1, dtmimp.OpRollback, bb.DBType, bb.BarrierTableName)
	var reason string
	if err == nil {
		queryTableName := bb.barrierTableName()
		querySQL := fmt.Sprintf("select reason from %s where gid=? and branch_id=? and op=? and barrier_id=?", queryTableName)
		querySQL = dtmimp.GetDBSpecial(bb.DBType).GetPlaceHoldSQL(querySQL)
		logger.Debugf("queryrow: %s", querySQL, bb.Gid, dtmimp.MsgDoBranch0, dtmimp.MsgDoOp, dtmimp.MsgDoBarrier1)
		err = db.QueryRow(querySQL, bb.Gid, dtmimp.MsgDoBranch0, dtmimp.MsgDoOp, dtmimp.MsgDoBarrier1).Scan(&reason)
	}
	if reason == dtmimp.OpRollback {
		return ErrFailure
	}
	return err
}
