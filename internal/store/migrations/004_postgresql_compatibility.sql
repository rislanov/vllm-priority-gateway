ALTER TABLE clients ADD COLUMN requests_per_minute INTEGER NOT NULL DEFAULT 0 CHECK (requests_per_minute BETWEEN 0 AND 10000000);
ALTER TABLE clients ADD COLUMN tokens_per_minute INTEGER NOT NULL DEFAULT 0 CHECK (tokens_per_minute BETWEEN 0 AND 10000000000000);
ALTER TABLE clients ADD COLUMN revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1);
ALTER TABLE model_pools ADD COLUMN revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1);
ALTER TABLE backends ADD COLUMN revision INTEGER NOT NULL DEFAULT 1 CHECK (revision >= 1);

CREATE TRIGGER clients_max_concurrency_insert
BEFORE INSERT ON clients WHEN NEW.max_concurrency > 10000
BEGIN SELECT RAISE(ABORT, 'max concurrency exceeds 10000'); END;

CREATE TRIGGER clients_max_concurrency_update
BEFORE UPDATE OF max_concurrency ON clients
WHEN NEW.max_concurrency > 10000 AND NEW.max_concurrency <> OLD.max_concurrency
BEGIN SELECT RAISE(ABORT, 'max concurrency exceeds 10000'); END;

CREATE TRIGGER pools_max_inflight_insert
BEFORE INSERT ON model_pools WHEN NEW.max_gateway_inflight > 100000
BEGIN SELECT RAISE(ABORT, 'max gateway inflight exceeds 100000'); END;

CREATE TRIGGER pools_max_inflight_update
BEFORE UPDATE OF max_gateway_inflight ON model_pools
WHEN NEW.max_gateway_inflight > 100000 AND NEW.max_gateway_inflight <> OLD.max_gateway_inflight
BEGIN SELECT RAISE(ABORT, 'max gateway inflight exceeds 100000'); END;
