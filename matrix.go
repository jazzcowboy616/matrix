package main

import (
	"errors"
	"fmt"
	"github.com/labstack/echo/v4"
	"mime/multipart"
	"path/filepath"
	"strings"
	"time"
)

const (
	readBufferSize  = 64 * 1024  // 64KB
	writeBufferSize = 128 * 1024 // 128KB
	maxProcessTime  = 15 * time.Minute

	Option_echo    = "Echo"
	Option_invert  = "Invert"
	Option_flatten = "Flatten"

	Method_Addition = "Addition"
	Method_Multiply = "Multiplication"
)

func Init(e *echo.Echo) {
	setController(e)
}

func setController(e *echo.Echo) {
	e.POST("/echo", func(c echo.Context) error { return Echo(c) })
	e.POST("/invert", func(c echo.Context) error { return Invert(c) })
	e.POST("/flatten", func(c echo.Context) error { return Flatten(c) })
	e.POST("/sum", func(c echo.Context) error { return Sum(c) })
	e.POST("/multiply", func(c echo.Context) error { return Multiply(c) })
}

func Echo(c echo.Context) error {
	return printMatrix(c, Option_echo)
}

func Invert(c echo.Context) error {
	return printMatrix(c, Option_invert)
}

func Flatten(c echo.Context) error {
	return printMatrix(c, Option_flatten)
}

func Sum(c echo.Context) error {
	return calcMatrix(c, Method_Addition)
}

func Multiply(c echo.Context) error {
	return calcMatrix(c, Method_Multiply)
}

func validateFileType(fileHeader *multipart.FileHeader) error {
	if ext := strings.ToLower(filepath.Ext(fileHeader.Filename)); ext != ".csv" {
		logger.Errorf("File type %s is not supported", ext)
		return fmt.Errorf("only csv file supported")
	}
	return nil
}

// Fetch fileHeader from multipart form to support stream read
func fetchFileHeader(form *multipart.Form) (*multipart.FileHeader, error) {
	files := form.File["file"]
	if len(files) == 0 {
		logger.Error("File not found in the form")
		return nil, errors.New("no files found in the form")
	}
	fileHeader := files[0]
	if fileHeader.Size == 0 {
		logger.Error("File is empty")
		return nil, errors.New("empty file")
	}
	if err := validateFileType(fileHeader); err != nil {
		return nil, err
	}
	return fileHeader, nil
}

// wrap source file and response
func prepareReaderWriter(c echo.Context, form *multipart.Form) (multipart.File, *echo.Response, error) {
	// get CSV handler
	fileHeader, err := fetchFileHeader(form)
	if err != nil {
		return nil, nil, err
	}

	// open file stream (not load into memory)
	srcFile, err := fileHeader.Open()
	if err != nil {
		logger.Error("failed to open source file: %v", err)
		return nil, nil, errors.New("fail to open file: " + err.Error())
	}

	// config stream response header
	resp := c.Response()
	resp.Header().Set(echo.HeaderContentType, "text/csv")
	resp.Header().Set(echo.HeaderContentEncoding, "chunked")
	logger.Debug("set response to stream output mode")

	return srcFile, resp, nil
}
