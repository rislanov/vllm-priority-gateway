package postgres

import "context"

func (c *TokenizationCoordinator) CleanupBatch(parent context.Context, limit int) (bool, error) {
	if limit < 1 || limit > 1000 {
		limit = 1000
	}
	ctx, cancel := context.WithTimeout(parent, c.timeout)
	defer cancel()
	tag, e := c.pool.Exec(ctx, `WITH expired AS (SELECT operation_id FROM tokenization_leases WHERE expires_at<=clock_timestamp() ORDER BY expires_at LIMIT $1 FOR UPDATE SKIP LOCKED) DELETE FROM tokenization_leases l USING expired e WHERE l.operation_id=e.operation_id`, limit)
	if e != nil {
		c.observe(e)
		return false, e
	}
	more := tag.RowsAffected() == int64(limit)
	tag, e = c.pool.Exec(ctx, `WITH expired AS (SELECT operation_id FROM tokenization_operations o WHERE retain_until<=clock_timestamp() AND NOT EXISTS(SELECT 1 FROM tokenization_leases l WHERE l.operation_id=o.operation_id) ORDER BY retain_until LIMIT $1 FOR UPDATE SKIP LOCKED) DELETE FROM tokenization_operations o USING expired e WHERE o.operation_id=e.operation_id`, limit)
	if e != nil {
		c.observe(e)
		return false, e
	}
	more = more || tag.RowsAffected() == int64(limit)
	// Scope/rate rows are small, one per configured entity; remove deleted clients
	// only after receipts have aged out, without disturbing active rate buckets.
	_, e = c.pool.Exec(ctx, `DELETE FROM tokenization_rate_state WHERE client_id IN (SELECT r.client_id FROM tokenization_rate_state r WHERE last_refill_at<clock_timestamp()-interval '24 hours' AND NOT EXISTS(SELECT 1 FROM clients c WHERE c.id=r.client_id) LIMIT $1)`, limit)
	c.observe(e)
	return more, e
}
