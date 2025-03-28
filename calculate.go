package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"github.com/labstack/echo/v4"
	"io"
	"math/big"
	"net/http"
	"strings"
)

// handle matrix calculation, support:
// addition and multiplication
func calcMatrix(c echo.Context, method string) error {
	ctx, cancel := context.WithTimeout(c.Request().Context(), maxProcessTime)
	defer cancel()

	form, err := c.MultipartForm()
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "form parse error: "+err.Error())
	}
	defer form.RemoveAll() // clear tmp file

	srcFile, resp, perr := prepareReaderWriter(c, form)
	if perr != nil {
		return echo.NewHTTPError(http.StatusBadRequest, err.Error())
	}
	defer srcFile.Close()

	if method == Method_Addition {
		if err = sumMatrix(ctx, srcFile, resp); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	} else if method == Method_Multiply {
		if err = multiplyMatrix(ctx, srcFile, resp); err != nil {
			return echo.NewHTTPError(http.StatusBadRequest, err.Error())
		}
	} else {
		return echo.NewHTTPError(http.StatusBadRequest, "invalid method: "+method)
	}
	return nil
}

// sum all the numbers in matrix
func sumMatrix(ctx context.Context, src io.Reader, resp *echo.Response) error {
	// initialize buffer
	bufferedReader := bufio.NewReaderSize(src, readBufferSize)

	// initialize csv parser
	csvReader := csv.NewReader(bufferedReader)
	csvReader.ReuseRecord = true
	csvReader.Comma = ','
	csvReader.FieldsPerRecord = 0

	csvWriter := csv.NewWriter(resp)
	defer csvWriter.Flush()

	// reuse bigInt to save the memory
	sum := new(big.Int)
	tmp := new(big.Int)
	for {
		select {
		case <-ctx.Done():
			{
				// context timeout and cancel, set status to 504
				err := ctx.Err()
				if errors.Is(err, context.DeadlineExceeded) {
					http.Error(resp, "Processing timeout", http.StatusGatewayTimeout)
				}
				return nil
			}
		default:
			record, cerr := csvReader.Read()
			if cerr != nil {
				if errors.Is(cerr, io.EOF) {
					csvWriter.Write([]string{sum.String()})
					csvWriter.Flush()
					return nil
				}
				logger.Error("failed to parse csv file")
				return echo.NewHTTPError(http.StatusBadRequest, "CSV parsing error: "+cerr.Error())
			}
			for _, num := range record {
				// validate format of the input, make sure all of them are valid number
				if _, succ := tmp.SetString(strings.TrimSpace(num), 10); !succ {
					logger.Error("failed to parse number: " + num)
					return echo.NewHTTPError(http.StatusBadRequest, num+" is not a number")
				}
				// use bigInt to handle huge file scenario, prevent from mathematics overflow
				sum.Add(sum, tmp)
			}
		}
	}
}

// multiply all the numbers in matrix
func multiplyMatrix(ctx context.Context, src io.Reader, resp *echo.Response) error {
	bufferedReader := bufio.NewReaderSize(src, readBufferSize)

	// initialize csv parser
	csvReader := csv.NewReader(bufferedReader)
	csvReader.ReuseRecord = true
	csvReader.Comma = ','

	csvWriter := csv.NewWriter(resp)
	defer csvWriter.Flush()

	product := big.NewInt(1)
	tmp := new(big.Int)
	for {
		select {
		case <-ctx.Done():
			{
				// context timeout and cancel, set status to 504
				err := ctx.Err()
				if errors.Is(err, context.DeadlineExceeded) {
					http.Error(resp, "Processing timeout", http.StatusGatewayTimeout)
				}
				return nil
			}
		default:
			record, cerr := csvReader.Read()
			if cerr != nil {
				// return final result when read out all data
				if errors.Is(cerr, io.EOF) {
					return csvWriter.Write([]string{product.String()})
				}
				logger.Error("failed to parse csv file")
				return echo.NewHTTPError(http.StatusBadRequest, "CSV parsing error: "+cerr.Error())
			}
			for _, num := range record {
				if _, succ := tmp.SetString(strings.TrimSpace(num), 10); !succ {
					logger.Error("failed to parse number: " + num)
					return echo.NewHTTPError(http.StatusBadRequest, num+" is not a number")
				}
				product.Mul(product, tmp)

				// optimize the loop, once it equals 0, directly  return to client
				if product.Cmp(big.NewInt(0)) == 0 {
					return csvWriter.Write([]string{"0"})
				}
			}
		}
	}
}
