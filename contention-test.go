package main

import (
	"flag"
	"fmt"
	"net"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

var (
	URL      = flag.String("mongo", "localhost", "URL for dialing mongo")
	Count    = flag.Int("count", 1000, "number to insert")
	MaxRetry = flag.Int("max-retry", 3, "retry an operation at most this many times")
	Start    = flag.Int("start", 0, "starting offset")
	Verify   = flag.Bool("verify", false, "After inserting, verify all documents are correct and new docs are inserted.")
	Timeout = flag.Int("timeout", 1000, "time in milliseconds to wait for connection timeout")
)

var RetryCount = 0

const ( // error codes
	// Toku Error Codes that can be retried
	tokuLockNotGranted = 16759
)

var retryCodes = map[int]bool{
	tokuLockNotGranted: true,
}

func isCodeRetry(code int) bool {
	return retryCodes[code]
}

func shouldRetry(err error, session *mgo.Session) bool {
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
	if netErr, ok := err.(*net.OpError); ok {
		fmt.Printf("OpError: %#v\n", netErr)
		if netErr.Temporary() || netErr.Timeout() {
			// tcp timeout errors need a refresh on the session so
			// we reconnect before we try again
			session.Refresh()
			return true
		}
		return false
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

func begin(db *mgo.Database) error {
	if err := transactionCommand(db, map[string]interface{}{"beginTransaction": 1}); err != nil {
		fmt.Printf("failed to beginTransaction: %s\n", err)
		return err
	}
	return nil
}

func commit(db *mgo.Database) error {
	if err := transactionCommand(db, "commitTransaction"); err != nil {
		fmt.Printf("failed to commitTransaction: %s\n", err)
		return err
	}
	return nil
}

func rollback(db *mgo.Database) error {
	if err := transactionCommand(db, "rollbackTransaction"); err != nil {
		// error is not printed, because we are probably already failing
		// fmt.Printf("failed to commitTransaction: %s\n", err)
		return err
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

func oneByOneClientTrans(session *mgo.Session) error {
	db := session.DB("test")
	txnCollection := db.C("transactions")
	var retries int = 0
	for i := 0; i < *Count; i++ {
		val := *Start + i
		// Reset retries per document
		for retries = 0; retries < *MaxRetry; retries++ {
			if err := begin(db); err != nil {
				if shouldRetry(err, session) {
					RetryCount += 1
					continue
				}
				return err
			}
			runner := txn.NewRunner(txnCollection)
			op := oneDocOp(val)
			txnId := bson.NewObjectId()
			if err := runner.Run([]txn.Op{op}, txnId, nil); err != nil {
				fmt.Printf("failed to insert %d\n%s\n", val, err)
				// ignore an error from rollback?
				if err2 := rollback(db); err2 != nil {
					// For now, don't print anything, because we're already in an error state.
					// fmt.Printf("failed to rollback while failing: %s\n", err2)
				}
				if shouldRetry(err, session) {
					RetryCount += 1
					continue
				}
				return err
			}
			if err := commit(db); err != nil {
				if shouldRetry(err, session) {
					RetryCount += 1
					continue
				}
				return err
			} else {
				break
			}
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
	session, err := mgo.DialWithTimeout(*URL, time.Duration(*Timeout) * time.Millisecond)
	if err != nil {
		panic(err)
	}
	session.SetSafe(&mgo.Safe{FSync: false})
	start := time.Now()
	err = oneByOneClientTrans(session)
	if err != nil {
		fmt.Printf("%.3fms to insert %d documents (%d retries, FAILED: %s)\n", float64(time.Since(start))/float64(time.Millisecond), *Count, RetryCount, err)
	} else {
		fmt.Printf("%.3fms to insert %d documents (%d retries)\n", float64(time.Since(start))/float64(time.Millisecond), *Count, RetryCount)
	}
	if *Verify {
		if err := verifyNewDocs(session.DB("test")); err != nil {
			fmt.Printf("verification failed: %s\n", err.Error())
			return
		}
		fmt.Printf("verification succeeded.\n")
	}
}
