package server

import (
	"net/http"

	"github.com/yashgorana/quxdb/pkg/version"
)

func indexHandler(w http.ResponseWriter, r *http.Request) {
	RenderPlainText(w, r, http.StatusOK, version.DetailedWithApp)
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
	RenderJSON(w, r, http.StatusOK, map[string]string{
		"status": "ok",
	})
}
