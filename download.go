package hcris-tools

import (
	"net/http"
)

func GetPage(url string) *http.Response {
	resp, err := http.Get(url)
	Check(err)

	return resp
}
