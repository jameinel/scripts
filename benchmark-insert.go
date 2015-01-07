package main

import (
	"flag"
	"fmt"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var URL = flag.String("mongo", "localhost", "URL for dialing mongo")
var Bulk = flag.Bool("bulk", false, "use a single bulk call")
var Count = flag.Int("count", 1000, "number to insert")
var Start = flag.Int("start", 0, "starting offset")
var Unsafe = flag.Bool("unsafe", false, "set safe mode to non-blocking and not-checking errors")
var Sync = flag.Bool("sync", false, "Force an fsync for writes to be considered committed (ignored if Unsafe is set)")

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
	collection := session.DB("test").C("foo")
	start := time.Now()
	if *Bulk {
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
	} else {
		for i := 0; i < *Count; i++ {
			val := *Start + i
			strVal := fmt.Sprintf("%04d", val)
			if err := collection.Insert(bson.M{"_id": strVal, "val": strVal}); err != nil {
				fmt.Printf("could not insert %d\n%s\n", val, err)
				return
			}
		}
	}
	fmt.Printf("%.3fms to insert %d documents\n", float64(time.Since(start)) / float64(time.Millisecond), *Count)
}
