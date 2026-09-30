ALTER TABLE model_pools ADD COLUMN high_priority_reserve INTEGER NOT NULL DEFAULT 0 CHECK (high_priority_reserve >= 0);

CREATE TRIGGER pools_priority_reserve_insert
BEFORE INSERT ON model_pools
WHEN NEW.high_priority_reserve > 0 AND (NEW.max_gateway_inflight <= 0 OR NEW.high_priority_reserve > NEW.max_gateway_inflight)
BEGIN SELECT RAISE(ABORT, 'high priority reserve requires a finite pool limit and cannot exceed it'); END;

CREATE TRIGGER pools_priority_reserve_update
BEFORE UPDATE OF high_priority_reserve, max_gateway_inflight ON model_pools
WHEN NEW.high_priority_reserve > 0 AND (NEW.max_gateway_inflight <= 0 OR NEW.high_priority_reserve > NEW.max_gateway_inflight)
BEGIN SELECT RAISE(ABORT, 'high priority reserve requires a finite pool limit and cannot exceed it'); END;
