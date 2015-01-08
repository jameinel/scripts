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
var MgoTrans = flag.Bool("mgo-trans", false, "Use client-side Mongo Transactions to update the database.")
var Verify = flag.Bool("verify", false, "After inserting, verify all documents are correct and new docs are inserted.")

func oneByOneDirect(db *mgo.Database) {
	collection := db.C("foo")
	for i := 0; i < *Count; i++ {
		val := *Start + i
		strVal := fmt.Sprintf("%04d", val)
		if err := collection.Insert(bson.M{"_id": strVal, "val": strVal}); err != nil {
			fmt.Printf("could not insert %d\n%s\n", val, err)
			return
		}
	}
}

func bulkDirect(db *mgo.Database) {
	collection := db.C("foo")
	docs := make([]interface{}, 0, *Count)
	for i := 0; i < *Count; i++ {
		val := *Start + i
		strVal := fmt.Sprintf("%04d", val)
		docs = append(docs, bson.M{"_id": strVal, "val": strVal})
	}
	if err := collection.Insert(docs...); err != nil {
		fmt.Printf("could not insert all docs\n%s\n", err.Error())
		return
	}
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

func oneByOneTxn(db *mgo.Database) {
	txnCollection := db.C("transactions")
	for i := 0; i < *Count; i++ {
		val := *Start + i
		runner := txn.NewRunner(txnCollection)
		op := oneDocOp(val)
		txnId := bson.NewObjectId()
		if err := runner.Run([]txn.Op{op}, txnId, nil); err != nil {
			fmt.Printf("could not insert %d\n%s\n", val, err)
			return
		}
	}
}

func bulkTxn(db *mgo.Database) {
	txnCollection := db.C("transactions")
	ops := make([]txn.Op, 0, *Count)
	for i := 0; i < *Count; i++ {
		ops = append(ops, oneDocOp(*Start+i))
	}
	runner := txn.NewRunner(txnCollection)
	txnId := bson.NewObjectId()
	if err := runner.Run(ops, txnId, nil); err != nil {
		fmt.Printf("could not insert all\n%s\n", err)
		return
	}
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
	session, err := mgo.Dial(*URL)
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
	switch {
	case *Bulk && *MgoTrans:
		bulkTxn(db)
	case *Bulk && !*MgoTrans:
		bulkDirect(db)
	case !*Bulk && *MgoTrans:
		oneByOneTxn(db)
	case !*Bulk && !*MgoTrans:
		oneByOneDirect(db)
	}
	fmt.Printf("%.3fms to insert %d documents\n", float64(time.Since(start))/float64(time.Millisecond), *Count)
	if *Verify {
		if err := verifyNewDocs(db); err != nil {
			fmt.Printf("verification failed: %s\n", err.Error())
		}
	}
}
