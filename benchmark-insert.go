package main

import (
	"flag"
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"gopkg.in/mgo.v2/txn"
)

var URL = flag.String("mongo", "localhost", "URL for dialing mongo")
var Bulk = flag.Bool("bulk", false, "use a single bulk call")
var Count = flag.Int("count", 1000, "number to insert")
var Start = flag.Int("start", 0, "starting offset")
var Unsafe = flag.Bool("unsafe", false, "set safe mode to non-blocking and not-checking errors")
var Sync = flag.Bool("sync", false, "Force an fsync for writes to be considered committed (ignored if Unsafe is set)")
var ClientTrans = flag.Bool("client-trans", false, "Use client-side Mongo Transactions (mgo/txn) to update the database.")
var Trans = flag.Bool("trans", false, "Use beginTransaction/commitTransaction calls.")
var Verify = flag.Bool("verify", false, "After inserting, verify all documents are correct and new docs are inserted.")

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

func maybeBegin(db *mgo.Database) error {
	if *Trans {
		if err := transactionCommand(db, "beginTransaction"); err != nil {
			fmt.Printf("failed to beginTransaction: %s\n", err)
			return err
		}
	}
	return nil
}

func maybeCommit(db *mgo.Database) error {
	if *Trans {
		if err := transactionCommand(db, "commitTransaction"); err != nil {
			fmt.Printf("failed to commitTransaction: %s\n", err)
			return err
		}
	}
	return nil
}

func maybeRollback(db *mgo.Database) error {
	if *Trans {
		if err := transactionCommand(db, "rollbackTransaction"); err != nil {
			// error is not printed, because we are probably already failing
			// fmt.Printf("failed to commitTransaction: %s\n", err)
			return err
		}
	}
	return nil
}

func oneByOneDirect(db *mgo.Database) {
	collection := db.C("foo")
	if maybeBegin(db) != nil {
		return
	}
	for i := 0; i < *Count; i++ {
		val := *Start + i
		strVal := fmt.Sprintf("%04d", val)
		if err := collection.Insert(bson.M{"_id": strVal, "val": strVal}); err != nil {
			fmt.Printf("could not insert %d\n%s\n", val, err)
			// ignore the error because we are returning anyway
			maybeRollback(db)
			return
		}
	}
	// ignore the error because we are returning anyway
	maybeCommit(db)
}

func bulkDirect(db *mgo.Database) {
	collection := db.C("foo")
	if maybeBegin(db) != nil {
		return
	}
	docs := make([]interface{}, 0, *Count)
	for i := 0; i < *Count; i++ {
		val := *Start + i
		strVal := fmt.Sprintf("%04d", val)
		docs = append(docs, bson.M{"_id": strVal, "val": strVal})
	}
	if err := collection.Insert(docs...); err != nil {
		fmt.Printf("could not insert all docs\n%s\n", err.Error())
		// ignore the error because we are returning anyway
		maybeRollback(db)
		return
	}
	// ignore the error because we are returning anyway
	maybeCommit(db)
}

func oneDocOp(val int) txn.Op {
	strVal := fmt.Sprintf("%04d", val)
	return txn.Op{
		C:      "foo",
		Id:     strVal,
		Assert: txn.DocMissing,
		Insert: bson.M{"_id": strVal, "val": strVal},
	}
}

func oneByOneClientTrans(db *mgo.Database) {
	txnCollection := db.C("transactions")
	if maybeBegin(db) != nil {
		return
	}
	for i := 0; i < *Count; i++ {
		val := *Start + i
		runner := txn.NewRunner(txnCollection)
		op := oneDocOp(val)
		txnId := bson.NewObjectId()
		if err := runner.Run([]txn.Op{op}, txnId, nil); err != nil {
			fmt.Printf("could not insert %d\n%s\n", val, err)
			// ignore the error because we are returning anyway
			maybeRollback(db)
			return
		}
	}
	// ignore the error because we are returning anyway
	maybeCommit(db)
}

func bulkClientTrans(db *mgo.Database) {
	txnCollection := db.C("transactions")
	ops := make([]txn.Op, 0, *Count)
	if maybeBegin(db) != nil {
		return
	}
	for i := 0; i < *Count; i++ {
		ops = append(ops, oneDocOp(*Start+i))
	}
	runner := txn.NewRunner(txnCollection)
	txnId := bson.NewObjectId()
	if err := runner.Run(ops, txnId, nil); err != nil {
		fmt.Printf("could not insert all\n%s\n", err)
		// ignore the error because we are returning anyway
		maybeRollback(db)
		return
	}
	// ignore the error because we are returning anyway
	maybeCommit(db)
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
		strVal := fmt.Sprintf("%04d", val)
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
	session, err := mgo.DialWithTimeout(*URL, time.Second)
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
	db := session.DB("test")
	start := time.Now()
	if *Bulk {
		if *ClientTrans {
			bulkClientTrans(db)
		} else {
			bulkDirect(db)
		}
	} else {
		if *ClientTrans {
			oneByOneClientTrans(db)
		} else {
			oneByOneDirect(db)
		}
	}
	fmt.Printf("%.3fms to insert %d documents\n", float64(time.Since(start))/float64(time.Millisecond), *Count)
	if *Verify {
		if err := verifyNewDocs(db); err != nil {
			fmt.Printf("verification failed: %s\n", err.Error())
			return
		}
		fmt.Printf("verification succeeded.\n")
	}
}
