package main

import (
	"flag"
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

var (
	URL         = flag.String("mongo", "localhost", "URL for dialing mongo")
	Bulk        = flag.Bool("bulk", false, "use a single bulk call")
	Count       = flag.Int("count", 1000, "number to insert")
	MaxRetry    = flag.Int("max-retry", 3, "retry an operation at most this many times")
	Start       = flag.Int("start", 0, "starting offset")
	Unsafe      = flag.Bool("unsafe", false, "set safe mode to non-blocking and not-checking errors")
	Sync        = flag.Bool("sync", false, "Force an fsync for writes to be considered committed (ignored if Unsafe is set)")
	ClientTrans = flag.Bool("client-trans", false, "Use client-side Mongo Transactions (mgo/txn) to update the database.")
	OneTrans    = flag.Bool("one-trans", false, "Use beginTransaction/commitTransaction across all inserts.")
	EachTrans   = flag.Bool("each-trans", false, "Use beginTransaction/commitTransaction across each single call (bulk or one-by-one each gets a separate transaction).")
	Verify      = flag.Bool("verify", false, "After inserting, verify all documents are correct and new docs are inserted.")
	Verbose     = flag.Bool("verbose", false, "Print more information.")
)

var RetryCount = 0

const ( // error codes
	fdbOperationFailed = 1000
	// These are all codes that "should be retried"
	fdbTimedOut      = 1004
	fdbPastVersion   = 1007
	fdbFutureVersion = 1009
	fdbNotCommitted  = 1020
	// Toku Error Codes that can be retried
	tokuLockNotGranted = 16759
)

var retryCodes = map[int]bool{
	fdbTimedOut:        true,
	fdbPastVersion:     true,
	fdbFutureVersion:   true,
	fdbNotCommitted:    true,
	tokuLockNotGranted: true,
}

func isCodeRetry(code int) bool {
	return retryCodes[code]
}

func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	if mgoErr, ok := err.(*mgo.LastError); ok {
		fmt.Printf("LastError: %#v\n", mgoErr)
		return isCodeRetry(mgoErr.Code)
	}
	if mgoErr, ok := err.(*mgo.QueryError); ok {
		fmt.Printf("QueryError: %#v\n", mgoErr)
		return isCodeRetry(mgoErr.Code)
	}
	return false
}

func transactionCommand(db *mgo.Database, cmd interface{}) error {
	var result bson.M
	err := db.Run(cmd, &result)
	if err != nil {
		return err
	} else if result["ok"].(float64) != 1 {
		return fmt.Errorf("%s did not return ok=1: %v\n", cmd, result)
	}
	return nil
}

func maybeBegin(db *mgo.Database, cond bool, retries *int) error {
	//maybeBegin takes 'retries' because we know no other work has been
	//done that we might want to clean up. so we just retry this specific
	//operation.
	if cond {
		// FoundationDB has a built-in 5s timeout for find() operations
		// operating inside of a transaction. So just set the max
		// timeout to 5s so we don't think we can get away with it when
		// running in single insert mode.
		// We set it to 6s so that we can see past_version failures
		// rather than transaction_timeout ones.
		for {
			if err := transactionCommand(db, map[string]interface{}{"beginTransaction": 1, "timeout": 6000}); err != nil {
				fmt.Printf("failed to beginTransaction: %s\n", err)
				if shouldRetry(err) {
					if retries != nil && *retries < *MaxRetry {
						*retries++
						RetryCount++
						continue
					}
				}
				return err
			}
			return nil
		}
	}
	return nil
}

func maybeCommit(db *mgo.Database, cond bool) error {
	if cond {
		if err := transactionCommand(db, "commitTransaction"); err != nil {
			fmt.Printf("failed to commitTransaction: %s\n", err)
			return err
		}
	}
	return nil
}

func maybeRollback(db *mgo.Database, cond bool) error {
	if cond {
		if err := transactionCommand(db, "rollbackTransaction"); err != nil {
			// error is not printed, because we are probably already failing
			// fmt.Printf("failed to commitTransaction: %s\n", err)
			return err
		}
	}
	return nil
}

func oneByOneDirect(db *mgo.Database) error {
	collection := db.C("foo")
	var retries int = 0
	if err := maybeBegin(db, *OneTrans, &retries); err != nil {
		return err
	}
	for i := 0; i < *Count; i++ {
		val := *Start + i
		strVal := fmt.Sprintf("%07d", val)
		// reset retries per document
		for retries = 0; retries < *MaxRetry; retries++ {
			if err := maybeBegin(db, *EachTrans, &retries); err != nil {
				return err
			}
			if err := collection.Insert(bson.M{"_id": strVal, "val": strVal}); err != nil {
				fmt.Printf("could not insert doc %d\n%s\n", val, err)
				// ignore the error because we are returning anyway
				maybeRollback(db, *OneTrans || *EachTrans)
				if shouldRetry(err) {
					RetryCount += 1
					continue
				}
				return err
			}
			if err := maybeCommit(db, *EachTrans); err != nil {
				if shouldRetry(err) {
					RetryCount += 1
					continue
				}
				return err
			} else {
				break
			}
		}
	}
	return maybeCommit(db, *OneTrans)
}

func bulkDirect(db *mgo.Database) error {
	collection := db.C("foo")
	var retries int = 0
	docs := make([]interface{}, 0, *Count)
	for i := 0; i < *Count; i++ {
		val := *Start + i
		strVal := fmt.Sprintf("%07d", val)
		docs = append(docs, bson.M{"_id": strVal, "val": strVal})
	}
	if *Verbose {
		fmt.Printf("First doc: %v\n", docs[0])
	}
	for ; retries < *MaxRetry; retries++ {
		if err := maybeBegin(db, *OneTrans || *EachTrans, &retries); err != nil {
			return err
		}
		if err := collection.Insert(docs...); err != nil {
			fmt.Printf("could not insert all docs\n%s\n", err.Error())
			// ignore the error because we are returning anyway
			maybeRollback(db, *OneTrans || *EachTrans)
			if shouldRetry(err) {
				RetryCount += 1
				continue
			}
			return err
		}
		if err := maybeCommit(db, *OneTrans || *EachTrans); err != nil {
			if shouldRetry(err) {
				RetryCount += 1
				continue
			}
			return err
		} else {
			break
		}
	}
	return nil
}

func oneDocOp(val int) txn.Op {
	strVal := fmt.Sprintf("%07d", val)
	return txn.Op{
		C:      "foo",
		Id:     strVal,
		Assert: txn.DocMissing,
		Insert: bson.M{"_id": strVal, "val": strVal},
	}
}

func oneByOneClientTrans(db *mgo.Database) error {
	txnCollection := db.C("transactions")
	var retries int = 0
	if err := maybeBegin(db, *OneTrans, &retries); err != nil {
		return err
	}
	for i := 0; i < *Count; i++ {
		val := *Start + i
		// Reset retries per document
		for retries = 0; retries < *MaxRetry; retries++ {
			if err := maybeBegin(db, *EachTrans, &retries); err != nil {
				return err
			}
			runner := txn.NewRunner(txnCollection)
			op := oneDocOp(val)
			txnId := bson.NewObjectId()
			if err := runner.Run([]txn.Op{op}, txnId, nil); err != nil {
				fmt.Printf("could not insert %d\n%s\n", val, err)
				// ignore the error because we are returning anyway
				maybeRollback(db, *OneTrans || *EachTrans)
				if shouldRetry(err) {
					RetryCount += 1
					continue
				}
				return err
			}
			if err := maybeCommit(db, *EachTrans); err != nil {
				if shouldRetry(err) {
					RetryCount += 1
					continue
				}
				return err
			} else {
				break
			}
		}
	}
	// ignore the error because we are returning anyway
	return maybeCommit(db, *OneTrans)
}

func bulkClientTrans(db *mgo.Database) error {
	txnCollection := db.C("transactions")
	var retries int = 0
	ops := make([]txn.Op, 0, *Count)
	for i := 0; i < *Count; i++ {
		ops = append(ops, oneDocOp(*Start+i))
	}
	for ; retries < *MaxRetry; retries++ {
		if err := maybeBegin(db, *OneTrans || *EachTrans, &retries); err != nil {
			return err
		}
		runner := txn.NewRunner(txnCollection)
		txnId := bson.NewObjectId()
		if err := runner.Run(ops, txnId, nil); err != nil {
			fmt.Printf("could not insert all\n%s\n", err)
			// ignore the error because we are returning anyway
			maybeRollback(db, *OneTrans || *EachTrans)
			if shouldRetry(err) {
				RetryCount += 1
				continue
			}
			return err
		}
		// ignore the error because we are returning anyway
		if err := maybeCommit(db, *OneTrans || *EachTrans); err != nil {
			if shouldRetry(err) {
				RetryCount += 1
				continue
			}
			return err
		} else {
			break
		}
	}
	return nil
}

type ValDoc struct {
	id  string `bson:"_id"`
	val string `bson:"val"`
}

func verifyNewDocs(db *mgo.Database) error {
	collection := db.C("foo")
	for i := 0; i < *Count; i++ {
		val := *Start + i
		var result ValDoc
		strVal := fmt.Sprintf("%07d", val)
		if err := collection.FindId(strVal).One(&result); err != nil {
			return fmt.Errorf("could not find document %d\n", val)
		}
		if result.id != result.val {
			return fmt.Errorf("document %s != %s", result.id, result.val)
		}
	}
	return nil
}

func main() {
	flag.Parse()
	session, err := mgo.DialWithTimeout(*URL, 10*time.Second)
	if err != nil {
		panic(err)
	}
	if *Unsafe {
		session.SetSafe(nil)
	} else if *Sync {
		// Turns out you can't set WMode: "majority" if you aren't in a replica set...
		session.SetSafe(&mgo.Safe{FSync: true})
	} else {
		session.SetSafe(&mgo.Safe{FSync: false})
	}
	if *OneTrans && *EachTrans {
		fmt.Printf("--one-trans and --each-trans are mutually exclusive\n")
		return
	}
	db := session.DB("test")
	start := time.Now()
	if *Verbose {
		fmt.Printf("inserting docs from %d to %d\n", *Start, *Start+*Count-1)
	}
	if *Bulk {
		if *ClientTrans {
			err = bulkClientTrans(db)
		} else {
			err = bulkDirect(db)
		}
	} else {
		if *ClientTrans {
			err = oneByOneClientTrans(db)
		} else {
			err = oneByOneDirect(db)
		}
	}
	if err != nil {
		fmt.Printf("%.3fms to insert %d documents (%d retries, FAILED: %s)\n", float64(time.Since(start))/float64(time.Millisecond), *Count, RetryCount, err)
	} else {
		fmt.Printf("%.3fms to insert %d documents (%d retries)\n", float64(time.Since(start))/float64(time.Millisecond), *Count, RetryCount)
	}
	if *Verify {
		if err := verifyNewDocs(db); err != nil {
			fmt.Printf("verification failed: %s\n", err.Error())
			return
		}
		fmt.Printf("verification succeeded.\n")
	}
}
