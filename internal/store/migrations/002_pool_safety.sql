ALTER TABLE model_pools ADD COLUMN max_gateway_inflight INTEGER NOT NULL DEFAULT 0 CHECK (max_gateway_inflight >= 0);
ALTER TABLE model_pools ADD COLUMN max_waiting INTEGER NOT NULL DEFAULT 0 CHECK (max_waiting >= 0);
