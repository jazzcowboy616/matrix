package main

import (
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

// Run with
//		go run .
// Send request with:
//		curl -F 'file=@/path/matrix.csv' "localhost:8080/echo"

var logger *zap.SugaredLogger

func InitLogger() {
	log, _ := zap.NewDevelopment()
	logger = log.Sugar()
}

func main() {
	e := echo.New()
	InitLogger()
	Init(e)

	if err := e.Start(":8080"); err != nil {
		logger.Fatal(err.Error())
	}
}
