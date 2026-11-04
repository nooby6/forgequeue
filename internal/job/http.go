package job

import (
    "encoding/json"
    "errors"
    "io"
    "net/http"
    "strings"
)

type Handler struct {
    service *Service
}

func NewHandler(service *Service) *Handler {
    return &Handler{service: service}
}

func (h *Handler) Create(w http.ResponseWriter, r *http.Request) {
    r.Body = http.MaxBytesReader(w, r.Body, 1<<20)

    var req CreateRequest
    decoder := json.NewDecoder(r.Body)
    decoder.DisallowUnknownFields()
    if err := decoder.Decode(&req); err != nil {
        writeError(w, http.StatusBadRequest, "invalid JSON body")
        return
    }

    var extra any
    if err := decoder.Decode(&extra); err != io.EOF {
        writeError(w, http.StatusBadRequest, "request body must contain one JSON value")
        return
    }

    if req.IdempotencyKey == "" {
        req.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
    }

    created, err := h.service.Create(r.Context(), req)
    if errors.Is(err, ErrDependencyCycle) {
        writeError(w, http.StatusConflict, ErrDependencyCycle.Error())
        return
    }
    if err != nil {
        writeError(w, http.StatusBadRequest, err.Error())
        return
    }
    writeJSON(w, http.StatusCreated, created)
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
    found, err := h.service.Get(r.Context(), r.PathValue("id"))
    if errors.Is(err, ErrNotFound) {
        writeError(w, http.StatusNotFound, "job not found")
        return
    }
    if err != nil {
        writeError(w, http.StatusInternalServerError, "failed to retrieve job")
        return
    }
    writeJSON(w, http.StatusOK, found)
}

func (h *Handler) Cancel(w http.ResponseWriter, r *http.Request) {
    cancelled, err := h.service.Cancel(r.Context(), r.PathValue("id"))
    if errors.Is(err, ErrNotFound) {
        writeError(w, http.StatusNotFound, "job not found")
        return
    }
    if errors.Is(err, ErrNotCancellable) {
        writeError(w, http.StatusConflict, "job cannot be cancelled in its current state")
        return
    }
    if err != nil {
        writeError(w, http.StatusInternalServerError, "failed to cancel job")
        return
    }
    writeJSON(w, http.StatusOK, cancelled)
}

func writeJSON(w http.ResponseWriter, status int, value any) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    _ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, message string) {
    writeJSON(w, status, map[string]string{"error": message})
}
