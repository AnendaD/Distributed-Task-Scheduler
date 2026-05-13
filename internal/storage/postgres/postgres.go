package postgres

import (
	"context"
	"errors"
	"time"

	"task-scheduler/internal/jobs"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository struct {
	pool *pgxpool.Pool
}

func NewPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 30
	cfg.MinConns = 1
	return pgxpool.NewWithConfig(ctx, cfg)
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) CreateJob(ctx context.Context, job *jobs.Job) (*jobs.Job, bool, error) {
	row := r.pool.QueryRow(ctx, `
		WITH inserted AS (
			INSERT INTO jobs (
				id, type, payload, status, run_at, attempts, max_attempts,
				idempotency_key, last_error, created_at, updated_at
			)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
			ON CONFLICT (idempotency_key) WHERE idempotency_key IS NOT NULL
			DO NOTHING
			RETURNING *, false AS existed
		)
		SELECT id, type, payload, status, run_at, attempts, max_attempts,
		       idempotency_key, last_error, created_at, updated_at, finished_at, existed
		FROM inserted
		UNION ALL
		SELECT id, type, payload, status, run_at, attempts, max_attempts,
		       idempotency_key, last_error, created_at, updated_at, finished_at, true AS existed
		FROM jobs
		WHERE idempotency_key = $8 AND $8 IS NOT NULL
		LIMIT 1`,
		job.ID, job.Type, job.Payload, job.Status, job.RunAt, job.Attempts, job.MaxAttempts,
		job.IdempotencyKey, job.LastError, job.CreatedAt, job.UpdatedAt,
	)
	var out jobs.Job
	var existed bool
	if err := scanJob(row, &out, &existed); err != nil {
		return nil, false, err
	}
	return &out, existed, nil
}

func (r *Repository) GetJob(ctx context.Context, id uuid.UUID) (*jobs.Job, error) {
	row := r.pool.QueryRow(ctx, `
		SELECT id, type, payload, status, run_at, attempts, max_attempts,
		       idempotency_key, last_error, created_at, updated_at, finished_at
		FROM jobs WHERE id = $1`, id)
	var job jobs.Job
	if err := scanJobNoExisted(row, &job); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, jobs.ErrNotFound
		}
		return nil, err
	}
	return &job, nil
}

func (r *Repository) SetStatus(ctx context.Context, id uuid.UUID, status jobs.Status) error {
	tag, err := r.pool.Exec(ctx, `UPDATE jobs SET status=$2, updated_at=now() WHERE id=$1`, id, status)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return jobs.ErrNotFound
	}
	return nil
}

func (r *Repository) MarkScheduled(ctx context.Context, id uuid.UUID, runAt time.Time, lastError *string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET status=$2, run_at=$3, last_error=$4, updated_at=now(), finished_at=NULL
		WHERE id=$1`,
		id, jobs.StatusScheduled, runAt, lastError)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return jobs.ErrNotFound
	}
	return nil
}

func (r *Repository) ClaimDueJobs(ctx context.Context, limit int) ([]jobs.Job, error) {
	tx, err := r.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		WITH picked AS (
			SELECT id
			FROM jobs
			WHERE status = $1 AND run_at <= now()
			ORDER BY run_at, created_at
			LIMIT $2
			FOR UPDATE SKIP LOCKED
		)
		UPDATE jobs j
		SET status = $3, updated_at = now()
		FROM picked
		WHERE j.id = picked.id
		RETURNING j.id, j.type, j.payload, j.status, j.run_at, j.attempts, j.max_attempts,
		          j.idempotency_key, j.last_error, j.created_at, j.updated_at, j.finished_at`,
		jobs.StatusScheduled, limit, jobs.StatusReady,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]jobs.Job, 0)
	for rows.Next() {
		var job jobs.Job
		if err := scanJobFromRows(rows, &job); err != nil {
			return nil, err
		}
		out = append(out, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Repository) MarkProcessing(ctx context.Context, id uuid.UUID) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs SET status=$2, updated_at=now()
		WHERE id=$1 AND status=$3`,
		id, jobs.StatusProcessing, jobs.StatusReady)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return jobs.ErrInvalidState
	}
	return nil
}

func (r *Repository) MarkSucceeded(ctx context.Context, id uuid.UUID) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET status=$2, last_error=NULL, updated_at=now(), finished_at=now()
		WHERE id=$1`,
		id, jobs.StatusSucceeded)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return jobs.ErrNotFound
	}
	return nil
}

func (r *Repository) ScheduleRetry(ctx context.Context, id uuid.UUID, runAt time.Time, attemptNumber int, lastError string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET status=$2, attempts=$3, run_at=$4, last_error=$5, updated_at=now()
		WHERE id=$1`,
		id, jobs.StatusScheduled, attemptNumber, runAt, lastError)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return jobs.ErrNotFound
	}
	return nil
}

func (r *Repository) MarkDead(ctx context.Context, id uuid.UUID, attempts int, lastError string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET status=$2, attempts=$3, last_error=$4, updated_at=now(), finished_at=now()
		WHERE id=$1`,
		id, jobs.StatusDead, attempts, lastError)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return jobs.ErrNotFound
	}
	return nil
}

func (r *Repository) CancelJob(ctx context.Context, id uuid.UUID) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE jobs
		SET status=$2, updated_at=now(), finished_at=now()
		WHERE id=$1 AND status IN ($3,$4,$5)`,
		id, jobs.StatusCancelled, jobs.StatusScheduled, jobs.StatusReady, jobs.StatusFailed)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		_, err := r.GetJob(ctx, id)
		if errors.Is(err, jobs.ErrNotFound) {
			return jobs.ErrNotFound
		}
		return jobs.ErrInvalidState
	}
	return nil
}

func (r *Repository) StartAttempt(ctx context.Context, jobID uuid.UUID, attemptNumber int) (uuid.UUID, error) {
	id := uuid.New()
	_, err := r.pool.Exec(ctx, `
		INSERT INTO job_attempts (id, job_id, attempt_number, status, started_at)
		VALUES ($1,$2,$3,$4,now())`,
		id, jobID, attemptNumber, jobs.StatusProcessing)
	return id, err
}

func (r *Repository) FinishAttempt(ctx context.Context, attemptID uuid.UUID, status string, errText *string) error {
	tag, err := r.pool.Exec(ctx, `
		UPDATE job_attempts
		SET status=$2, error=$3, finished_at=now()
		WHERE id=$1`,
		attemptID, status, errText)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return jobs.ErrNotFound
	}
	return nil
}

func (r *Repository) CleanupExpiredJobs(ctx context.Context, olderThanDays int) (int64, error) {
	if olderThanDays <= 0 {
		olderThanDays = 30
	}
	tag, err := r.pool.Exec(ctx, `
		DELETE FROM jobs
		WHERE status IN ($1,$2)
		  AND finished_at IS NOT NULL
		  AND finished_at < now() - make_interval(days => $3)`,
		jobs.StatusSucceeded, jobs.StatusDead, olderThanDays)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

func scanJob(row pgx.Row, job *jobs.Job, existed *bool) error {
	return row.Scan(
		&job.ID, &job.Type, &job.Payload, &job.Status, &job.RunAt, &job.Attempts, &job.MaxAttempts,
		&job.IdempotencyKey, &job.LastError, &job.CreatedAt, &job.UpdatedAt, &job.FinishedAt, existed,
	)
}

func scanJobNoExisted(row pgx.Row, job *jobs.Job) error {
	return row.Scan(
		&job.ID, &job.Type, &job.Payload, &job.Status, &job.RunAt, &job.Attempts, &job.MaxAttempts,
		&job.IdempotencyKey, &job.LastError, &job.CreatedAt, &job.UpdatedAt, &job.FinishedAt,
	)
}

func scanJobFromRows(rows pgx.Rows, job *jobs.Job) error {
	return rows.Scan(
		&job.ID, &job.Type, &job.Payload, &job.Status, &job.RunAt, &job.Attempts, &job.MaxAttempts,
		&job.IdempotencyKey, &job.LastError, &job.CreatedAt, &job.UpdatedAt, &job.FinishedAt,
	)
}
