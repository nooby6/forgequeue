package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
)

type QueueStats struct {
	QueueName       string     `json:"queue_name"`
	Pending         int64      `json:"pending"`
	Running         int64      `json:"running"`
	Succeeded       int64      `json:"succeeded"`
	Failed          int64      `json:"failed"`
	Cancelled       int64      `json:"cancelled"`
	ActiveDeadLetters int64    `json:"active_dead_letters"`
	OldestPendingAt *time.Time `json:"oldest_pending_at"`
}

// Stats returns a point-in-time summary of job states for one queue. It uses
// database-side aggregates so the API never loads individual jobs into memory.
func (q *PostgresQueue) Stats(ctx context.Context, queueName string) (QueueStats, error) {
	stats := QueueStats{QueueName: queueName}
	var oldest pgtype.Timestamptz
	err := q.db.QueryRow(ctx, `
		SELECT
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'running'),
			COUNT(*) FILTER (WHERE status = 'succeeded'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'cancelled'),
			MIN(created_at) FILTER (WHERE status = 'pending')
		FROM jobs
		WHERE queue_name = $1
	`, queueName).Scan(
		&stats.Pending, &stats.Running, &stats.Succeeded,
		&stats.Failed, &stats.Cancelled, &oldest,
	)
	if err != nil {
		return QueueStats{}, fmt.Errorf("query queue statistics: %w", err)
	}
	if oldest.Valid {
		value := oldest.Time
		stats.OldestPendingAt = &value
	}

	err = q.db.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM dead_letter_jobs
		WHERE queue_name = $1 AND replayed_at IS NULL
	`, queueName).Scan(&stats.ActiveDeadLetters)
	if err != nil {
		return QueueStats{}, fmt.Errorf("query active dead-letter count: %w", err)
	}
	return stats, nil
}
