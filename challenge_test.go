package main

import (
	"bytes"
	"context"
	"io"
	"math/big"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/stretchr/testify/assert"
)

func TestHandlers(t *testing.T) {
	e := echo.New()
	InitLogger()
	Init(e)

	tests := []struct {
		name       string
		endpoint   string
		input      string
		wantStatus int
		wantBody   string
		formKey    string
	}{
		// Echo测试用例
		{
			name:       "Echo valid matrix",
			endpoint:   "/echo",
			input:      "1,2\n3,4",
			wantStatus: http.StatusOK,
			wantBody:   "1,2\n3,4\n",
		},
		// Invert测试用例
		{
			name:       "Invert valid matrix",
			endpoint:   "/invert",
			input:      "1,2\n3,4",
			wantStatus: http.StatusOK,
			wantBody:   "1,3\n2,4\n",
		},

		// Flatten测试用例
		{
			name:       "Flatten matrix",
			endpoint:   "/flatten",
			input:      "1,2\n3,4",
			wantStatus: http.StatusOK,
			wantBody:   "1,2,3,4\n",
		},

		// Sum测试用例
		{
			name:       "Sum valid numbers",
			endpoint:   "/sum",
			input:      "1,2\n3,4",
			wantStatus: http.StatusOK,
			wantBody:   "10\n",
		},
		{
			name:       "Sum invalid number",
			endpoint:   "/sum",
			input:      "1,a\n3,4",
			wantStatus: http.StatusBadRequest,
			wantBody:   "not a number",
		},

		// Multiply测试用例
		{
			name:       "Multiply valid numbers",
			endpoint:   "/multiply",
			input:      "2,3\n4,5",
			wantStatus: http.StatusOK,
			wantBody:   "120\n",
		},
		{
			name:       "Multiply with zero",
			endpoint:   "/multiply",
			input:      "2,0\n3,4",
			wantStatus: http.StatusOK,
			wantBody:   "0\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 构造multipart请求
			body := &bytes.Buffer{}
			writer := multipart.NewWriter(body)
			part, _ := writer.CreateFormFile("file", "test.csv")
			io.Copy(part, strings.NewReader(tt.input))
			writer.Close()

			req := httptest.NewRequest(http.MethodPost, tt.endpoint, body)
			req.Header.Set("Content-Type", writer.FormDataContentType())
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantStatus == http.StatusOK {
				assert.Equal(t, tt.wantBody, rec.Body.String())
			} else {
				assert.Contains(t, rec.Body.String(), tt.wantBody)
			}
		})
	}

	// 特殊测试用例
	t.Run("Empty file handling", func(t *testing.T) {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, _ := writer.CreateFormFile("file", "empty.csv")
		io.Copy(part, strings.NewReader(""))
		writer.Close()

		req := httptest.NewRequest(http.MethodPost, "/echo", body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("Invalid content type", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/echo", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("Large numbers handling", func(t *testing.T) {
		bigNum := "123456789012345678901234567890"
		input := strings.Join([]string{bigNum, bigNum}, ",")

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, _ := writer.CreateFormFile("file", "big.csv")
		io.Copy(part, strings.NewReader(input))
		writer.Close()

		// test Sum
		req := httptest.NewRequest(http.MethodPost, "/sum", body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		expectedSum := new(big.Int)
		expectedSum.SetString(bigNum, 10)
		expectedSum.Mul(expectedSum, big.NewInt(2))
		assert.Contains(t, rec.Body.String(), expectedSum.String())

		part, _ = writer.CreateFormFile("file", "big.csv")
		io.Copy(part, strings.NewReader(input))
		writer.Close()

		// tes Multiply
		req = httptest.NewRequest(http.MethodPost, "/multiply", body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		rec = httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		expectedProduct := new(big.Int)
		expectedProduct.SetString(bigNum, 10)
		expectedProduct.Mul(expectedProduct, expectedProduct)
		assert.Contains(t, rec.Body.String(), expectedProduct.String())
	})

	t.Run("Timeout handling", func(t *testing.T) {
		// 创建需要长时间处理的超大输入
		bigInput := strings.Repeat("1,2,3,4,5,6\n", 10000000)

		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, _ := writer.CreateFormFile("file", "huge.csv")
		io.Copy(part, strings.NewReader(bigInput))
		writer.Close()

		// 创建带超时的context
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Microsecond)
		defer cancel()

		req := httptest.NewRequest(http.MethodPost, "/echo", body)
		req = req.WithContext(ctx)
		req.Header.Set("Content-Type", writer.FormDataContentType())

		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusGatewayTimeout, rec.Code)
	})

	t.Run("Invalid file type", func(t *testing.T) {
		body := &bytes.Buffer{}
		writer := multipart.NewWriter(body)
		part, _ := writer.CreateFormFile("file", "test.txt")
		io.Copy(part, strings.NewReader("invalid content"))
		writer.Close()

		req := httptest.NewRequest(http.MethodPost, "/echo", body)
		req.Header.Set("Content-Type", writer.FormDataContentType())
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "only csv file supported")
	})
}
