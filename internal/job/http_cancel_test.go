package job

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type cancellationTestRepository struct {
	job Job
	err error
	id  string
}

func (r *cancellationTestRepository) Create(context.Context, CreateRequest) (Job, error) {
	return Job{}, errors.New("not implemented")
}

func (r *cancellationTestRepository) Get(context.Context, string) (Job, error) {
	return Job{}, ErrNotFound
}

func (r *cancellationTestRepository) Cancel(_ context.Context, id string) (Job, error) {
	r.id = id
	return r.job, r.err
}

func TestCancelHandlerReturnsCancelledJob(t *testing.T) {
	repo := &cancellationTestRepository{
		job: Job{ID: "job-1", Status: StatusCancelled, UpdatedAt: time.Now()},
	}
	handler := NewHandler(NewService(repo))
	request := httptest.NewRequest(http.MethodDelete, "/jobs/job-1", nil)
	request.SetPathValue("id", "job-1")
	response := httptest.NewRecorder()

	handler.Cancel(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d; want %d", response.Code, http.StatusOK)
	}
	if repo.id != "job-1" {
		t.Fatalf("cancelled id = %q; want job-1", repo.id)
	}
	if !strings.Contains(response.Body.String(), `"status":"cancelled"`) {
		t.Fatalf("response body does not contain cancelled status: %s", response.Body.String())
	}
}

func TestCancelHandlerMapsMissingAndNonCancellableJobs(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		wantStatus int
		wantBody   string
	}{
		{name: "missing", err: ErrNotFound, wantStatus: http.StatusNotFound, wantBody: "job not found"},
		{name: "running", err: ErrNotCancellable, wantStatus: http.StatusConflict, wantBody: "job cannot be cancelled in its current state"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &cancellationTestRepository{err: tt.err}
			handler := NewHandler(NewService(repo))
			request := httptest.NewRequest(http.MethodDelete, "/jobs/job-1", nil)
			request.SetPathValue("id", "job-1")
			response := httptest.NewRecorder()

			handler.Cancel(response, request)

			if response.Code != tt.wantStatus {
				t.Fatalf("status = %d; want %d", response.Code, tt.wantStatus)
			}
			if !strings.Contains(response.Body.String(), tt.wantBody) {
				t.Fatalf("body = %q; want it to contain %q", response.Body.String(), tt.wantBody)
			}
		})
	}
}
