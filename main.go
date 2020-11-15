
package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
)

func main() {
	f, err := os.Open("train.csv")
	handleErr(err)
	headers, data, indices, err := ingest(f)
	handleErr(err)
	c := cardinality(indices)

	fmt.Printf("Original Data: \nRows: %d, Cols: %d\n========\n", len(data), len(headers))
	for i, h := range headers {
		fmt.Printf("%v: %v\n", h, c[i])
	}
	fmt.Println("")
}

func handleErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// counts the number of of unique values in a column, assuming that the index i of indices represents a column
func cardinality(indices []map[string][]int) []int {
	cardinalities := make([]int, len(indices))
	for i, val := range indices {
		cardinalities[i] = len(val)
	}
	return cardinalities
}

// ingests the file and outputs the header, data, and indices
func ingest(f io.Reader) (header []string, data [][]string, indices []map[string][]int, err error) {
	r := csv.NewReader(f)

	//handle header
	if header, err = r.Read(); err != nil {
		return
	}

	indices = make([]map[string][]int, len(header))
	rowCount, colCount := 0, len(header)
	for rec, err := r.Read(); err == nil; rec, err = r.Read() {
		if len(rec) != colCount {
			return nil, nil, nil, fmt.Errorf("Expected Columns: %d. Got %d columns in row %d", colCount, len(rec), rowCount)
		}
		// handle data
		data = append(data, rec)

		// handle indices
		for j, val := range rec {
			if indices[j] == nil {
				indices[j] = make(map[string][]int)
			}
			indices[j][val] = append(indices[j][val], rowCount)
		}
		rowCount++
	}
	return
}