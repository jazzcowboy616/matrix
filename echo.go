package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/labstack/echo/v4"
	"io"
	"net/http"
)

// write chunk to the csvWrite
func flushChunk(w *csv.Writer, data []string) error {
	for len(data) > 0 {
		// max 10000 for each
		batchSize := minimum(10000, len(data))
		line := data[:batchSize]
		if err := w.Write(line); err != nil {
			return err
		}
		data = data[batchSize:]
	}
	w.Flush()
	return w.Error()
}

func minimum(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// handle matrix print, support:
// echo or flatten
func printMatrix(c echo.Context, printOption string) error {
	// generate context with specific timeout
	ctx, cancel := context.WithTimeout(c.Request().Context(), maxProcessTime)
	defer cancel()

	form, err := c.MultipartForm()
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "form parse error: "+err.Error())
	}
	defer form.RemoveAll() // clear tmp file

	srcFile, resp, perr := prepareReaderWriter(c, form)
	if perr != nil {
		logger.Errorf("prepare reader error: %v", perr)
		return echo.NewHTTPError(http.StatusBadRequest, perr.Error())
	}
	defer srcFile.Close()

	switch printOption {
	case Option_echo:
		{
			return echoMatrix(ctx, srcFile, resp)
		}
	case Option_invert:
		{
			return invertMatrix(ctx, srcFile, resp)
		}
	case Option_flatten:
		{
			return flattenMatrix(ctx, srcFile, resp)
		}
	default:
		return echoMatrix(ctx, srcFile, resp)
	}
}

// echo matrix by using io stream to support big file
func echoMatrix(ctx context.Context, src io.Reader, resp *echo.Response) error {
	// initialize buffer
	bufferedReader := bufio.NewReaderSize(src, readBufferSize)

	// initialize csv parser
	csvReader := csv.NewReader(bufferedReader)
	csvReader.ReuseRecord = true
	csvReader.Comma = ','
	csvReader.FieldsPerRecord = -1 // not force to have same columns each line

	bufferedWriter := bufio.NewWriterSize(resp, writeBufferSize)

	// initialize csvWriter with buffer
	csvWriter := csv.NewWriter(bufferedWriter)
	defer csvWriter.Flush()

	var (
		expectedCols int
		rowCount     int
	)
	for {
		select {
		case <-ctx.Done():
			{
				// context timeout and cancel, set status to 504
				err := ctx.Err()
				if errors.Is(err, context.DeadlineExceeded) {
					logger.Errorf("Processing echo matrix timeout")
					return echo.NewHTTPError(http.StatusGatewayTimeout, "Processing timeout")
				}
				return nil
			}
		default:
			record, cerr := csvReader.Read()
			if cerr != nil {
				// check the EOF to complete the reading
				if errors.Is(cerr, io.EOF) {
					// check if the input is a matrix
					if rowCount != expectedCols {
						msg := fmt.Sprintf("Not a matrix: line: %d, columns: %d", rowCount, expectedCols)
						logger.Errorf(msg)
						return echo.NewHTTPError(http.StatusBadRequest, msg)
					}
					// Flush to response before return, header is committed, status code cannot be changed anymore
					csvWriter.Flush()
					return nil // normal ended
				}
				logger.Errorf("fail to parse csv record: %v", cerr)
				return echo.NewHTTPError(http.StatusBadRequest, "CSV parsing error: "+cerr.Error())
			}

			// determine the expected column number by first row's columns
			if rowCount == 0 {
				expectedCols = len(record)
			}

			// return error if the crrent line doesn't have same length of column as first row
			if len(record) != expectedCols {
				msg := fmt.Sprintf("column number inconsistent: row: %d expects %d colums", rowCount+1, expectedCols)
				logger.Errorf(msg)
				return echo.NewHTTPError(http.StatusBadRequest, msg)
			}

			if cerr = csvWriter.Write(record); cerr != nil {
				logger.Errorf("fail to write csv record: %v", cerr)
				return echo.NewHTTPError(http.StatusInternalServerError, "writing response error: "+cerr.Error())
			}

			// flush to buffer every 1000 rows
			if rowCount > 0 && rowCount%1000 == 0 {
				// ** There is a potential issue, once this code is executed, http header is committed and http status code cannot be changed anymore.
				csvWriter.Flush()
				if cerr = csvWriter.Error(); cerr != nil {
					return cerr
				}
				resp.Flush() // force flush to client
			}
			rowCount++
		}
	}
}

// flatten matrix to one line, using io stream and buffer to support big file
func flattenMatrix(ctx context.Context, src io.Reader, resp *echo.Response) error {
	// initialize buffer
	bufferedReader := bufio.NewReaderSize(src, readBufferSize)

	// initialize csv parser
	csvReader := csv.NewReader(bufferedReader)
	csvReader.ReuseRecord = true
	csvReader.Comma = ','

	bufferedWriter := bufio.NewWriterSize(resp, writeBufferSize)

	csvWriter := csv.NewWriter(bufferedWriter)

	var (
		flattenedRecord []string
		expectedCols    int
		rowCount        int
	)
	for {
		select {
		case <-ctx.Done():
			{
				// context timeout and cancel, set status to 504
				err := ctx.Err()
				if errors.Is(err, context.DeadlineExceeded) {
					logger.Errorf("Processing flatten matrix timeout")
					return echo.NewHTTPError(http.StatusGatewayTimeout, "Processing timeout")
				}
				return nil
			}
		default:
			record, cerr := csvReader.Read()
			if cerr != nil {
				if errors.Is(cerr, io.EOF) {
					// check the EOF to complete the reading
					if rowCount != expectedCols {
						msg := fmt.Sprintf("Not a matrix: line: %d, columns: %d", rowCount, expectedCols)
						logger.Errorf(msg)
						return echo.NewHTTPError(http.StatusBadRequest, msg)
					}
					// write the last remained data
					if len(flattenedRecord) > 0 {
						if err := csvWriter.Write(flattenedRecord); err != nil {
							logger.Errorf("fail to write csv record: %v", cerr)
							return echo.NewHTTPError(http.StatusInternalServerError, "writing response error: "+err.Error())
						}
					}
					// Flush
					csvWriter.Flush()
					return nil // normal ended
				}
				return echo.NewHTTPError(http.StatusBadRequest, "CSV parsing error: "+cerr.Error())
			}

			// return error if the crrent line doesn't have same length of column as first row
			if rowCount == 0 {
				expectedCols = len(record)
			}

			// return error if the crrent line doesn't have same length of column as first row
			if len(record) != expectedCols {
				msg := fmt.Sprintf("column number inconsistent: row: %d expects %d colums", rowCount+1, expectedCols)
				logger.Errorf(msg)
				return echo.NewHTTPError(http.StatusBadRequest, msg)
			}

			flattenedRecord = append(flattenedRecord, record...)
			rowCount++

			// force flush every 100000 characters
			if len(flattenedRecord) >= 100000 {
				if err := flushChunk(csvWriter, flattenedRecord); err != nil {
					return err
				}
				flattenedRecord = flattenedRecord[:0] // clean slice
			}
		}
	}
}
