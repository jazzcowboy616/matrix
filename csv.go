package main

import (
	"encoding/csv"
	"math/rand"
	"os"
	"strconv"
	"time"
)

func BigCVS() {
	// create csv file
	file, err := os.Create("random_matrix.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// init csv.Writer instance
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// set random seed
	rand.Seed(time.Now().UnixNano())

	var matrixNum = 2000
	// generate the matrix
	for i := 0; i < matrixNum; i++ {
		row := make([]string, matrixNum)
		for j := 0; j < matrixNum; j++ {
			// generate random number between -100 and 100
			// if the num is 0, redo
			for {
				num := rand.Intn(201) - 100
				if num != 0 {
					row[j] = strconv.Itoa(num)
					break
				}
			}
		}
		// output to file
		err = writer.Write(row)
		if err != nil {
			panic(err)
		}
	}
}
