package job

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type schedulingTestRepository struct {
	request CreateRequest
	job Job
	err error
}

func (r *schedulingTestRepository) Create(_ context.Context, req CreateRequest) (Job, error) {
	r.request = req
	return r.job, r.err
}

func (r *schedulingTestRepository) Get(context.Context, string) (Job, error) {
	return Job{}, ErrNotFound
}

func (r *schedulingTestRepository) Cancel(context.Context, string) (Job, error) {
	return Job{}, errors.New("not implemented")
}

func TestCreatePreservesScheduledTime(t *testing.T) {
	runAt := time.Date(2027, 2, 3, 12, 30, 0, 0, time.UTC)
	repo := &schedulingTestRepository{}
	service := NewService(repo)

	_, err := service.Create(context.Background(), CreateRequest{
		QueueName: " default ",
		JobType: " report ",
		Payload: json.RawMessage(`{"kind":"daily"}`),
		ScheduledAt: &runAt,
	})
	if err != nil {
		t.Fatalf("create scheduled job: %v", err)
	}
	if repo.request.ScheduledAt == nil || !repo.request.ScheduledAt.Equal(runAt) {
		t.Fatalf("scheduled_at = %v; want %v", repo.request.ScheduledAt, runAt)
	}
	if repo.request.QueueName != "default" || repo.request.JobType != "report" {
		t.Fatalf("request was not normalized: %+v", repo.request)
	}
}

func TestCreateHandlerAcceptsScheduledAt(t *testing.T) {
	runAt := time.Date(2027, 2, 3, 12, 30, 0, 0, time.UTC)
	repo := &schedulingTestRepository{
		job: Job{ID: "job-1", QueueName: "reports", JobType: "daily-report", Status: StatusPending, AvailableAt: runAt},
	}
	handler := NewHandler(NewService(repo))
	body := `{"queue_name":"reports","job_type":"daily-report","payload":{"kind":"daily"},"scheduled_at":"2027-02-03T12:30:00Z"}`
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	response := httptest.NewRecorder()

	handler.Create(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("status = %d; want %d; body=%s", response.Code, http.StatusCreated, response.Body.String())
	}
	if repo.request.ScheduledAt == nil || !repo.request.ScheduledAt.Equal(runAt) {
		t.Fatalf("scheduled_at = %v; want %v", repo.request.ScheduledAt, runAt)
	}
	if !strings.Contains(response.Body.String(), `"available_at":"2027-02-03T12:30:00Z"`) {
		t.Fatalf("response does not expose the scheduled availability time: %s", response.Body.String())
	}
}

func TestCreateHandlerRejectsInvalidScheduledAt(t *testing.T) {
	repo := &schedulingTestRepository{}
	handler := NewHandler(NewService(repo))
	body := `{"queue_name":"reports","job_type":"daily-report","payload":{},"scheduled_at":"next Tuesday"}`
	request := httptest.NewRequest(http.MethodPost, "/jobs", strings.NewReader(body))
	response := httptest.NewRecorder()

	handler.Create(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d; want %d", response.Code, http.StatusBadRequest)
	}
	if repo.request.ScheduledAt != nil {
		t.Fatalf("repository should not be called for invalid timestamp")
	}
}
