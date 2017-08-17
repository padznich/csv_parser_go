package main

import (
	"database/sql"
	"log"
	"os"
	"strings"

	"github.com/fatih/structs"
	"github.com/gocarina/gocsv"
)

func main() {

	csv_file := "test.csv"
	db_driver := "clickhouse"
	db_uri := "tcp://web1:9000?username=default&password=default&database=csv_parser_db"
	db_table_nmae := "test"

	// get data from csv-file
	data := csv_reader(csv_file)
	// write parsed data into DB
	insert_many(db_driver, db_uri, db_table_nmae, data)

}

func insert_many(db_driver string, db_uri string, table_name string, data []*Test) {

	// identify columns
	fields := structs.Names(&Test{})
	// columns as a string for sql-query
	_columns_str := strings.ToLower(strings.Join(fields, ", "))
	// columns number for sql-query
	_columns_number := len(fields)

	// create query
	query := "INSERT INTO " + table_name + "(" + _columns_str + ")" + " VALUES " +
		"(" + strings.Repeat("?, ", _columns_number)[:_columns_number*3-2] + ")"

	// DB connect
	connect, err := sql.Open(db_driver, db_uri)
	if err != nil {
		log.Fatal(err)
	}

	// cretae cursor
	var (
		tx, _     = connect.Begin()
		cursor, _ = tx.Prepare(query)
	)

	// create transaction
	for _, row := range data {
		cursor.Exec(row.ID, row.A, row.B, row.C, row.D) // TODO: avoid hardcode
	}

	// commit
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	log.Println("DB insertions SUCCESS")

}

func csv_reader(csv_file string) []*Test {

	file, err := os.OpenFile(csv_file, os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	parsed_csv := []*Test{}

	if err := gocsv.UnmarshalFile(file, &parsed_csv); err != nil {
		panic(err)
	}

	log.Println("CSV parsed")
	return parsed_csv

}

type Test struct {
	ID int64  `csv:"id"`
	A  string `csv:"a"`
	B  string `csv:"b"`
	C  string `csv:"c"`
	D  string `csv:"d"`
}
