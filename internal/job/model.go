package job

import("encoding/json";"time")
type Status string
const(StatusPending Status="pending";StatusRunning Status="running";StatusSucceeded Status="succeeded";StatusFailed Status="failed";StatusCancelled Status="cancelled")
type Job struct{ID string `json:"id"`;IdempotencyKey string `json:"idempotency_key,omitempty"`;QueueName string `json:"queue_name"`;JobType string `json:"job_type"`;Payload json.RawMessage `json:"payload"`;Status Status `json:"status"`;Priority int `json:"priority"`;Attempts int `json:"attempts"`;AvailableAt time.Time `json:"available_at"`;CreatedAt time.Time `json:"created_at"`;UpdatedAt time.Time `json:"updated_at"`}
type CreateRequest struct{QueueName string `json:"queue_name"`;JobType string `json:"job_type"`;Payload json.RawMessage `json:"payload"`;Priority int `json:"priority"`;IdempotencyKey string `json:"idempotency_key,omitempty"`}
