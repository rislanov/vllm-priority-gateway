ALTER TABLE model_pools ADD COLUMN high_priority_reserve INTEGER NOT NULL DEFAULT 0,
 ADD CONSTRAINT model_pools_priority_reserve_check CHECK
 (high_priority_reserve >= 0 AND (high_priority_reserve = 0 OR (max_gateway_inflight > 0 AND high_priority_reserve <= max_gateway_inflight)));

-- Historical leases have no recorded class. Count them conservatively as lower priority.
ALTER TABLE request_leases ADD COLUMN priority_class TEXT NOT NULL DEFAULT 'normal'
 CHECK (priority_class IN ('critical', 'high', 'normal', 'background'));
