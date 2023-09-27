package main

import (
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type Benchmark struct {
	ID   int
	XStr string
	YStr string
	ZStr string
	XInt int
	YInt int
	ZInt int
}

func main() {
	fmt.Println("test gorm")
	dsn := "host=localhost user=testuser password=12345 dbname=ferretdb port=5432 sslmode=disable"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		fmt.Println(err)
	}

	var points []Benchmark
	db.Find(&points, "attributes @> '{\"xstr\": [\"117\"]}'")

	for _, p := range points {
		fmt.Printf("%+v\n", p)
	}

}
