package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/labstack/echo/v4"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
)

const (
	tempDir      = "./"
	blockSize    = 6               // no of rows handled each time
	tmpFileCount = 3               // no of temporary files
	bufferSize   = 4 * 1024 * 1024 // 每个文件4MB缓冲区
)

type TempFileHelper struct {
	tempFiles   []*os.File
	tempWriters []*csv.Writer
	colRanges   [][2]int
	colNum      int
}

// initialize helper
func NewTempFileHelper(tempDir string, totalCols int) (*TempFileHelper, error) {
	// Calculate columns for each temp file
	baseCols := totalCols / tmpFileCount
	extraCols := totalCols % tmpFileCount

	// generate column range for each temp file
	colRanges := make([][2]int, tmpFileCount)
	currentCol := 0
	cols := baseCols
	// handle extra column if totalCols % temp file number > 0
	if extraCols > 0 {
		cols++
	}
	logger.Debugf("The matrix will be devided into several %d * %d blocks", blockSize, cols)
	for i := 0; i < tmpFileCount; i++ {
		colRanges[i] = [2]int{currentCol, currentCol + cols}
		currentCol += cols
	}

	th := &TempFileHelper{colRanges: colRanges}
	// create temp files handler and writer, and store in tempFile array
	for i := 0; i < tmpFileCount; i++ {
		file, err := os.CreateTemp(tempDir, fmt.Sprintf("invert_%d_*.tmp", i))
		if err != nil {
			return nil, err
		}
		th.tempFiles = append(th.tempFiles, file)
		th.tempWriters = append(th.tempWriters, csv.NewWriter(bufio.NewWriterSize(file, bufferSize)))
		th.colNum = colRanges[i][1] - colRanges[i][0] // use to read from temp file and assemble
	}

	return th, nil
}

/**
 * invert block in memory, turns to blockSize x (totalColumns/tmpFileCount)
 * e.q.:
 *									[1,11]										[6,16]
 * 		[1,2,3,4,5]					[2,12]			[6,7,8,9,10]				[7,17]
 *		[11,12,13,14,15]	-->		[3,13]			[16,17,18,19,20]	-->		[8,18]
 *									[4,14]										[9,19]
 *									[5,15]										[10,20]
 */
func invertInMemory(matrix [][]string) [][]string {
	if len(matrix) == 0 {
		return nil
	}
	rows := len(matrix)
	cols := len(matrix[0])

	result := make([][]string, cols)
	for i := range result {
		result[i] = make([]string, rows)
	}

	for i := 0; i < rows; i++ {
		for j := 0; j < cols; j++ {
			result[j][i] = matrix[i][j]
		}
	}
	return result
}

// handle single block, (totalColumns/tmpFileCount) * blockSize
func (th *TempFileHelper) ProcessBlock(block [][]string) error {
	// invert one block in memory
	invertedMatrix := invertInMemory(block)

	// sharded and write into temp file
	for fileIdx := 0; fileIdx < tmpFileCount; fileIdx++ {
		placeholderNum := 0
		start := th.colRanges[fileIdx][0]
		end := th.colRanges[fileIdx][1]
		if start >= len(invertedMatrix) {
			break
		}
		if end > len(invertedMatrix) {
			// for those extra columns, need to calculate the stakehold to align the row
			placeholderNum = end - len(invertedMatrix) + 1
			end = len(invertedMatrix)
		}

		// write block into corresponding temp file
		for _, col := range invertedMatrix[start:end] {
			if err := th.tempWriters[fileIdx].Write(col); err != nil {
				return err
			}
		}
		// write a row with a space as a placeholder
		for i := 0; i < placeholderNum-1; i++ {
			if err := th.tempWriters[fileIdx].Write([]string{" "}); err != nil {
				return err
			}
		}
		th.tempWriters[fileIdx].Flush()
	}
	return nil
}

// Read data from temp files and assemble
func (th *TempFileHelper) StreamOutput(w io.Writer) error {
	csvWriter := csv.NewWriter(w)
	defer csvWriter.Flush()

	// create readers for each tmp file
	readers := make([]*csv.Reader, tmpFileCount)
	for i, file := range th.tempFiles {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			return err
		}
		readers[i] = csv.NewReader(bufio.NewReader(file))
		readers[i].FieldsPerRecord = -1 // ignore fields inconsistent
	}

	// assemble data from temp file
	row := make([][]string, th.colNum)
	hasData := false

	// iterate all temp file, wrap the final rows
	for fileIdx := 0; fileIdx < len(th.tempFiles); fileIdx++ {
		logger.Debugf("Start reading file-%d...", fileIdx)
		// read all file managed columns
		for colIdx := 0; ; colIdx++ {
			record, err := readers[fileIdx].Read()
			if errors.Is(err, io.EOF) {
				// move to next temp file
				break
			}
			if err != nil {
				return err
			}
			hasData = true
			// handle placeholder row
			if len(record) == 1 && strings.TrimSpace(record[0]) == "" {
				continue
			}
			row[colIdx%th.colNum] = append(row[colIdx%th.colNum], record...)
		}

		if !hasData {
			break
		}
		for i := 0; i < th.colNum; i++ {
			if err := csvWriter.Write(row[i]); err != nil {
				return err
			}
		}
		row = make([][]string, th.colNum) // clean row to reuse in next file
		hasData = false
	}
	return nil
}

// invert matrix
func invertMatrix(ctx context.Context, src io.Reader, resp *echo.Response) error {
	tmpDir, err := os.MkdirTemp(tempDir, "matrix_invert")
	if err != nil {
		logger.Errorf("failed to create temporary directory: %w", err)
		return fmt.Errorf("fail to create directory: %w", err)
	}

	// get column number
	previewReader := csv.NewReader(src)
	firstRow, err := previewReader.Read()
	if err != nil {
		return err
	}
	totalCols := len(firstRow)
	logger.Debugf("The matrix has %d columns.", totalCols)

	// reset reading point
	if _, err = src.(multipart.File).Seek(0, io.SeekStart); err != nil {
		logger.Errorf("fail to reset reading point: %w", err)
		return errors.New("fail to reset reading point: " + err.Error())
	}

	logger.Debug("Start to initialize temp file helper...")
	helper, err := NewTempFileHelper(tmpDir, totalCols)
	if err != nil {
		logger.Errorf("fail to initialize temp file: %w", err)
		return fmt.Errorf("fail to init temp file helper: %w", err)
	}

	// initialize reader
	block := make([][]string, 0, blockSize)
	reader := csv.NewReader(src)

	csvWriter := csv.NewWriter(resp)

	defer func() {
		csvWriter.Flush()
		//helper.Close()
		//os.RemoveAll(tmpDir)
	}()

	// read blocks and write into temp files
BlockLoop:
	for {
		select {
		case <-ctx.Done():
			{
				err := ctx.Err()
				if errors.Is(err, context.DeadlineExceeded) {
					http.Error(resp, "Processing timeout", http.StatusGatewayTimeout)
				}
				return nil
			}
		default:
			record, rerr := reader.Read()
			if rerr == io.EOF {
				if len(block) > 0 {
					if perr := helper.ProcessBlock(block); perr != nil {
						return perr
					}
				}
				break BlockLoop
			}
			if rerr != nil {
				return rerr
			}

			block = append(block, record)
			if len(block) == blockSize {
				if perr := helper.ProcessBlock(block); perr != nil {
					return perr
				}
				block = block[:0] // 清空缓冲
			}
		}
	}

	// set steam output
	resp.Header().Set("Content-Type", "text/csv")
	return helper.StreamOutput(resp.Writer)
}

// Close and remove the temp files
func (tp *TempFileHelper) Close() error {
	var firstErr error
	for idx, file := range tp.tempFiles {
		// close writer
		if writer := tp.tempWriters[idx]; writer != nil {
			writer.Flush()
			if err := writer.Error(); err != nil && firstErr == nil {
				firstErr = err
			}
		}

		// close and delete temp file
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		if err := os.Remove(file.Name()); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
