package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TxFunc is the unit of work executed inside a transaction.
type TxFunc func(tx pgx.Tx) error

// WithTx runs fn inside a transaction. It commits on success, rolls back on
// error or panic (re-panicking after rollback).
func WithTx(ctx context.Context, pool *pgxpool.Pool, fn TxFunc) error {
	return withTxOpts(ctx, pool, pgx.TxOptions{}, fn)
}

// WithTxOpts is like WithTx but lets you specify isolation level / access mode.
func withTxOpts(ctx context.Context, pool *pgxpool.Pool, opts pgx.TxOptions, fn TxFunc) error {
	tx, err := pool.BeginTx(ctx, opts)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback(ctx)
			panic(p) // re-panic after cleanup
		}
	}()

	if err := fn(tx); err != nil {
		if rbErr := tx.Rollback(ctx); rbErr != nil {
			return fmt.Errorf("tx error: %w; rollback error: %v", err, rbErr)
		}
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// WithSerializableTx runs fn in SERIALIZABLE isolation — use for outbox writes.
func WithSerializableTx(ctx context.Context, pool *pgxpool.Pool, fn TxFunc) error {
	return withTxOpts(ctx, pool, pgx.TxOptions{
		IsoLevel:   pgx.Serializable,
		AccessMode: pgx.ReadWrite,
	}, fn)
}

// WithRepeatableReadTx is useful for read-then-write patterns.
func WithRepeatableReadTx(ctx context.Context, pool *pgxpool.Pool, fn TxFunc) error {
	return withTxOpts(ctx, pool, pgx.TxOptions{
		IsoLevel: pgx.RepeatableRead,
	}, fn)
}

/// Usage:
//err := db.WithTx(ctx, dbHandler.Pool(), func(ctx context.Context, tx pgx.Tx) error {
//	if err := orderRepo.Create(ctx, tx, order); err != nil {
//		return err
//	}
//	return outboxRepo.Publish(ctx, tx, "order.created", order)
//})
