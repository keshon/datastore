// Command orders is a complete, runnable datastore program.
//
// It is the README's quickstart, kept here so the compiler checks it: an
// example that has drifted out of date is worse than no example.
//
//	go run ./examples/orders
package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/keshon/datastore"
)

// A record is an ordinary struct. The only requirement is Key(), returning
// something unique and stable — it is what addresses the record in the log.
type Order struct {
	ID       uint64    `json:"id"`
	Customer string    `json:"customer"`
	Email    string    `json:"email"`
	Total    int       `json:"total"`
	Created  time.Time `json:"created"`
}

func (o *Order) Key() string { return strconv.FormatUint(o.ID, 10) }

type Customer struct {
	ID    string `json:"id"`
	Bonus int    `json:"bonus"`
}

func (c *Customer) Key() string { return c.ID }

func main() {
	dir, err := os.MkdirTemp("", "datastore-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// 1. Describe the database. This touches no disk.
	db := datastore.New(datastore.Options{Dir: dir})

	// 2. Declare collections and indexes BEFORE opening: replaying the log has
	//    to know where records belong and which indexes to rebuild.
	orders := datastore.Register[*Order](db, "orders")
	customers := datastore.Register[*Customer](db, "customers")

	byCustomer := datastore.AddIndex(orders, "customer",
		func(o *Order) []string { return []string{o.Customer} })
	byEmail := datastore.AddUnique(orders, "email",
		func(o *Order) string { return o.Email })
	byDate := datastore.AddSorted(orders, "date",
		func(o *Order) int64 { return o.Created.Unix() })

	// 3. Open: take the directory lock, load the snapshot, replay the log,
	//    rebuild the indexes.
	if err := db.Open(); err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Open succeeding is not proof that nothing was lost — a damaged log tail
	// is discarded so the store can start at all. Ask.
	if rep := db.Recovery(); rep.LostData() {
		log.Printf("datastore lost committed work: %s", rep.TruncatedReason)
	}

	if err := db.Update(func(tx *datastore.Tx) error {
		return datastore.In(tx, customers).Put(&Customer{ID: "cust-7", Bonus: 500})
	}); err != nil {
		log.Fatal(err)
	}

	// 4. The transaction that justifies the package: an order placed and a
	//    loyalty balance debited, together or not at all. One frame, one fsync.
	err = db.Update(func(tx *datastore.Tx) error {
		o := datastore.In(tx, orders)
		c := datastore.In(tx, customers)

		cust, ok := c.Get("cust-7")
		if !ok {
			return fmt.Errorf("no such customer")
		}
		cust.Bonus -= 100
		if err := c.Put(cust); err != nil {
			return err
		}

		tx.Note("actor", "user:7") // recorded in the log, readable via History

		return o.Put(&Order{
			ID:       tx.NextID("order"), // permanent only if this commits
			Customer: "cust-7",
			Email:    "buyer@example.com",
			Total:    4200,
			Created:  time.Now(),
		})
	})
	if err != nil {
		log.Fatal(err)
	}

	// 5. Read. No transaction needed, and every value is a fresh copy.
	if o, ok := orders.Get("1"); ok {
		fmt.Printf("order 1: %s, %d\n", o.Customer, o.Total)
	}
	if c, ok := customers.Get("cust-7"); ok {
		fmt.Printf("bonus after the order: %d\n", c.Bonus)
	}

	fmt.Println("orders for cust-7:", len(byCustomer.Find("cust-7")))
	if o, ok := byEmail.Get("buyer@example.com"); ok {
		fmt.Println("found by email:", o.ID)
	}
	for _, o := range byDate.Desc(10, 0) {
		fmt.Println("recent:", o.ID, o.Total)
	}

	// All decodes one record per step, so breaking out stops the work.
	for o := range byCustomer.All("cust-7") {
		fmt.Println("first match:", o.ID)
		break
	}

	fmt.Printf("%d orders, %d bytes of records\n", orders.Len(), db.Stats().Bytes)
}
