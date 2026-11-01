package job

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

type scheduledRequestRepository struct {
	request CreateRequest
	called  bool
}

func (r *scheduledRequestRepository) Create(_ context.Context, req CreateRequest) (Job, error) {
	r.called = true
	r.request = req
	return Job{}, nil
}

func (r *scheduledRequestRepository) Get(context.Context, string) (Job, error) {
	return Job{}, nil
}

func (r *scheduledRequestRepository) Cancel(context.Context, string) (Job, error) {
	return Job{}, nil
}

func TestCreatePassesAvailableAtToRepository(t *testing.T) {
	availableAt := time.Date(2026, time.November, 2, 9, 0, 0, 0, time.UTC)
	repo := &scheduledRequestRepository{}
	service := NewService(repo)

	_, err := service.Create(context.Background(), CreateRequest{
		QueueName:   "reports",
		JobType:     "daily-report",
		Payload:     json.RawMessage(`{}`),
		MaxAttempts: 3,
		AvailableAt: &availableAt,
	})
	if err != nil {
		t.Fatalf("create scheduled job: %v", err)
	}
	if !repo.called {
		t.Fatal("expected repository to be called")
	}
	if repo.request.AvailableAt == nil || !repo.request.AvailableAt.Equal(availableAt) {
		t.Fatalf("repository available_at = %v; want %s", repo.request.AvailableAt, availableAt)
	}
}

func TestCreateRejectsZeroAvailableAt(t *testing.T) {
	repo := &scheduledRequestRepository{}
	service := NewService(repo)
	zero := time.Time{}

	_, err := service.Create(context.Background(), CreateRequest{
		QueueName:   "reports",
		JobType:     "daily-report",
		Payload:     json.RawMessage(`{}`),
		MaxAttempts: 3,
		AvailableAt: &zero,
	})
	if err == nil {
		t.Fatal("expected zero available_at to be rejected")
	}
	if repo.called {
		t.Fatal("repository must not be called for an invalid available_at")
	}
}
