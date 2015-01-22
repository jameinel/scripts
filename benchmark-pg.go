package main

import (
	"database/sql"
	"flag"
	"fmt"
	"time"

	"github.com/lib/pq"
)

var (
	User      = flag.String("user", "", "username to connect as")
	Password  = flag.String("password", "", "password for the user")
	Prepared  = flag.Bool("prepared", false, "use a prepared statement for one-by-one insertion")
	CopyFrom  = flag.Bool("copyfrom", false, "use COPY FROM instead of INSERT")
	Count     = flag.Int("count", 1000, "number to insert")
	Start     = flag.Int("start", 0, "starting offset")
	OneTrans  = flag.Bool("one-trans", false, "Use beginTransaction/commitTransaction across all inserts.")
	EachTrans = flag.Bool("each-trans", false, "Use beginTransaction/commitTransaction across each single call (bulk or one-by-one each gets a separate transaction).")
	Verify    = flag.Bool("verify", false, "After inserting, verify all documents are correct and new docs are inserted.")
	Verbose   = flag.Bool("verbose", false, "Print more information.")
	Setup     = flag.Bool("setup", false, "Just create the tables and exit")
	Cleanup   = flag.Bool("cleanup", false, "destroy all the tables and exit")
)

var RetryCount = 0

type TxOrDB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Prepare(query string) (*sql.Stmt, error)
}

func oneByOneDirect(db *sql.DB) error {
	var execer TxOrDB
	var tx *sql.Tx
	var prepared *sql.Stmt
	if *OneTrans {
		var err error
		tx, err = db.Begin()
		if err != nil {
			// retry?
			return err
		}
		execer = tx
	} else {
		execer = db
	}
	if *Prepared {
		var err error
		prepared, err = execer.Prepare("INSERT INTO foo (id, val) VALUES ($1, $2)")
		if err != nil {
			return err
		}
	} else {
		execer = db
	}
	for i := 0; i < *Count; i++ {
		val := *Start + i
		strVal := fmt.Sprintf("%07d", val)
		var err error
		if prepared != nil {
			_, err = prepared.Exec(val, strVal)
		} else {
			if *EachTrans {
				var err error
				tx, err = db.Begin()
				if err != nil {
					return err
				}
				execer = tx
			} else {
				execer = db
			}
			_, err = execer.Exec("INSERT INTO foo (id, val) VALUES ($1, $2)", val, strVal)
		}
		if err != nil {
			fmt.Printf("could not insert doc %d\n%s\n", val, err)
			if tx != nil {
				tx.Rollback()
			}
			return err
		}
		if *EachTrans {
			if err := tx.Commit(); err != nil {
				return err
			}
		}
	}
	if *OneTrans {
		return tx.Commit()
	}
	return nil
}

// func bulkDirect(db *sql.DB) error {
// 	collection := db.C("foo")
// 	var retries int = 0
// 	docs := make([]interface{}, 0, *Count)
// 	for i := 0; i < *Count; i++ {
// 		val := *Start + i
// 		strVal := fmt.Sprintf("%07d", val)
// 		docs = append(docs, bson.M{"_id": strVal, "val": strVal})
// 	}
// 	if *Verbose {
// 		fmt.Printf("First doc: %v\n", docs[0])
// 	}
// 	for ; retries < *MaxRetry; retries++ {
// 		if err := maybeBegin(db, *OneTrans || *EachTrans, &retries); err != nil {
// 			return err
// 		}
// 		if err := collection.Insert(docs...); err != nil {
// 			fmt.Printf("could not insert all docs\n%s\n", err.Error())
// 			// ignore the error because we are returning anyway
// 			maybeRollback(db, *OneTrans || *EachTrans)
// 			if shouldRetry(err) {
// 				RetryCount += 1
// 				continue
// 			}
// 			return err
// 		}
// 		if err := maybeCommit(db, *OneTrans || *EachTrans); err != nil {
// 			if shouldRetry(err) {
// 				RetryCount += 1
// 				continue
// 			}
// 			return err
// 		} else {
// 			break
// 		}
// 	}
// 	return nil
// }

type ValDoc struct {
	val string `bson:"val"`
}

func setupDirectTable(db *sql.DB) error {
	_, err := db.Exec("CREATE TABLE foo(id INTEGER PRIMARY KEY, val TEXT)")
	return err
}

func setupJSONTable(db *sql.DB) error {
	_, err := db.Exec("CREATE TABLE fooJSON (id INTEGER PRIMARY KEY, val JSON)")
	return err
}

func cleanupTables(db *sql.DB) error {
	if _, err := db.Exec("DROP TABLE foo"); err != nil {
		if pqerr, ok := err.(*pq.Error); ok {
			if pqerr.Code.Name() == "undefined_table" {
				// dropping a table that doesn't exist is fine
				fmt.Printf("tables already deleted\n")
				return nil
			}
		}
		return err
	} else {
		fmt.Printf("tables deleted\n")
	}
	return nil
}

func verifyNewDocs(db *sql.DB) error {
	rows, err := db.Query(
		"SELECT id, val"+
			" FROM foo"+
			" WHERE id >= ? AND id < ?"+
			" ORDER BY id INCR",
		*Start, *Start+*Count)
	if err != nil {
		return err
	}
	expected := *Start
	for rows.Next() {
		var id int
		var val string
		if err = rows.Scan(&id, &val); err != nil {
			return err
		}
		if id != expected {
			return fmt.Errorf("expected id %d found %d", expected, id)
		}
		strVal := fmt.Sprintf("%07d", id)
		if val != strVal {
			return fmt.Errorf("expected id %d to have val %s not %s", expected, strVal, val)
		}
		expected++
	}
	return rows.Err()
}

func main() {
	flag.Parse()
	connString := "sslmode=disable"
	if *User != "" {
		connString += fmt.Sprintf(" user='%s'", *User)
	}
	if *Password != "" {
		connString += fmt.Sprintf(" password='%s'", *Password)
	}
	db, err := sql.Open("postgres", connString)
	if err != nil {
		fmt.Printf("Failed to open database: %s\n", err)
		return
	}
	if *OneTrans && *EachTrans {
		fmt.Printf("--one-trans and --each-trans are mutually exclusive\n")
		return
	}
	if *EachTrans && *Prepared {
		fmt.Printf("--each-trans and --prepare doesn't make sense\n")
		return
	}
	if *Cleanup {
		if err := cleanupTables(db); err != nil {
			fmt.Printf("failed to clean up tables: %s\n", err)
		}
	}
	if *Setup {
		if err := setupDirectTable(db); err != nil {
			fmt.Printf("failed to set up table: %s\n", err)
		} else {
			fmt.Printf("set up direct tables\n")
		}
	}
	if *Cleanup || *Setup {
		return
	}
	if *Verbose {
		fmt.Printf("inserting docs from %d to %d\n", *Start, *Start+*Count-1)
	}
	start := time.Now()
	err = oneByOneDirect(db)
	if err != nil {
		fmt.Printf("%.3fms to insert %d documents (FAILED: %s)\n", float64(time.Since(start))/float64(time.Millisecond), *Count, err)
	} else {
		fmt.Printf("%.3fms to insert %d documents\n", float64(time.Since(start))/float64(time.Millisecond), *Count)
	}
	if *Verify {
		if err := verifyNewDocs(db); err != nil {
			fmt.Printf("verification failed: %s\n", err.Error())
			return
		}
		fmt.Printf("verification succeeded.\n")
	}
}
