package api

import (
	"encoding/json"
	"net/http"
)

type MetricsHTTPServer struct {
	Server *Server
}

func NewMetricsHTTPServer(
	server *Server,
) *MetricsHTTPServer {

	return &MetricsHTTPServer{
		Server: server,
	}
}

func (m *MetricsHTTPServer) MetricsHandler(
	w http.ResponseWriter,
	r *http.Request,
) {

	if r.Method != http.MethodGet {

		http.Error(
			w,
			"method not allowed",
			http.StatusMethodNotAllowed,
		)

		return
	}

	if m.Server == nil {

		http.Error(
			w,
			"metrics server is not initialized",
			http.StatusInternalServerError,
		)

		return
	}

	snapshot := m.Server.MetricsSnapshot()

	w.Header().Set(
		"Content-Type",
		"application/json",
	)

	w.WriteHeader(
		http.StatusOK,
	)

	if err := json.NewEncoder(w).Encode(snapshot); err != nil {
		return
	}
}

func MetricsHandlerFromServer(
	server *Server,
) http.Handler {

	mux := http.NewServeMux()

	metricsServer :=
		NewMetricsHTTPServer(server)

	mux.HandleFunc(
		"/metrics",
		metricsServer.MetricsHandler,
	)

	return mux
}
