package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	basestore "github.com/rislanov/vllm-priority-gateway/internal/store"
)

func (s *Store) mutation(ctx context.Context, fn func(pgx.Tx) error) error {
	tx, err := s.config.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.New("begin PostgreSQL configuration transaction")
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, "SET LOCAL synchronous_commit = on"); err != nil {
		return errors.New("set PostgreSQL configuration durability")
	}
	if err = fn(tx); err != nil {
		return err
	}
	var revision int64
	if err = tx.QueryRow(ctx, "UPDATE config_meta SET revision=revision+1 WHERE singleton=1 RETURNING revision").Scan(&revision); err != nil {
		return errors.New("increment PostgreSQL configuration revision")
	}
	if _, err = tx.Exec(ctx, "SELECT pg_notify('llmgw_config_changed', $1::text)", fmt.Sprint(revision)); err != nil {
		return errors.New("notify PostgreSQL configuration change")
	}
	if err = tx.Commit(ctx); err != nil {
		return errors.New("commit PostgreSQL configuration transaction")
	}
	return nil
}

func (s *Store) CreateClient(ctx context.Context, p basestore.CreateClientParams) (domain.Client, error) {
	v := domain.Client{Name: p.Name, Enabled: p.Enabled, PriorityClass: p.PriorityClass, VLLMPriority: p.VLLMPriority, MaxConcurrency: p.MaxConcurrency, RequestsPerMinute: p.RequestsPerMinute, TokensPerMinute: p.TokensPerMinute, Revision: 1}
	if err := v.Validate(); err != nil {
		return domain.Client{}, err
	}
	now := time.Now().UTC()
	v.CreatedAt = now
	v.UpdatedAt = now
	err := s.mutation(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO clients(name,enabled,priority_class,vllm_priority,max_concurrency,requests_per_minute,tokens_per_minute,created_at,updated_at) VALUES($1::text,$2::boolean,$3::text,$4::integer,$5::integer,$6::bigint,$7::bigint,$8::timestamptz,$8::timestamptz) RETURNING id`, v.Name, v.Enabled, v.PriorityClass, v.VLLMPriority, v.MaxConcurrency, v.RequestsPerMinute, v.TokensPerMinute, now).Scan(&v.ID); err != nil {
			return errors.New("insert PostgreSQL client")
		}
		if _, err := tx.Exec(ctx, "INSERT INTO client_admission_scopes(client_id,updated_at) VALUES($1::bigint,$2::timestamptz)", v.ID, now); err != nil {
			return errors.New("create PostgreSQL client admission scope")
		}
		return replaceAccess(ctx, tx, v.ID, p.ModelPoolIDs)
	})
	return v, err
}

func (s *Store) UpdateClient(ctx context.Context, id int64, p basestore.UpdateClientParams) (domain.Client, error) {
	v := domain.Client{ID: id, Name: p.Name, Enabled: p.Enabled, PriorityClass: p.PriorityClass, VLLMPriority: p.VLLMPriority, MaxConcurrency: p.MaxConcurrency, RequestsPerMinute: p.RequestsPerMinute, TokensPerMinute: p.TokensPerMinute}
	err := s.mutation(ctx, func(tx pgx.Tx) error {
		// Access changes can affect both the previous and requested pool set.
		// Configuration mutations are rare, so locking every pool scope in the
		// canonical order is a deliberately conservative way to make that union
		// stable while admission remains deadlock-free.
		if _, err := tx.Exec(ctx, "SELECT pool_id FROM pool_admission_scopes ORDER BY pool_id FOR UPDATE"); err != nil {
			return errors.New("lock PostgreSQL pool admission scopes")
		}
		if _, err := tx.Exec(ctx, "SELECT client_id FROM client_admission_scopes WHERE client_id=$1::bigint FOR UPDATE", id); err != nil {
			return errors.New("lock PostgreSQL client admission scope")
		}
		var previous domain.Client
		if err := tx.QueryRow(ctx, "SELECT max_concurrency,revision,created_at FROM clients WHERE id=$1::bigint FOR UPDATE", id).Scan(&previous.MaxConcurrency, &previous.Revision, &v.CreatedAt); err != nil {
			return err
		}
		if err := v.ValidateUpdate(previous); err != nil {
			return err
		}
		v.Revision = previous.Revision + 1
		v.UpdatedAt = time.Now().UTC()
		tag, err := tx.Exec(ctx, `UPDATE clients SET name=$2::text,enabled=$3::boolean,priority_class=$4::text,vllm_priority=$5::integer,max_concurrency=$6::integer,requests_per_minute=$7::bigint,tokens_per_minute=$8::bigint,revision=revision+1,updated_at=$9::timestamptz WHERE id=$1::bigint`, id, v.Name, v.Enabled, v.PriorityClass, v.VLLMPriority, v.MaxConcurrency, v.RequestsPerMinute, v.TokensPerMinute, v.UpdatedAt)
		if err != nil || tag.RowsAffected() != 1 {
			return errors.New("update PostgreSQL client")
		}
		return replaceAccess(ctx, tx, id, p.ModelPoolIDs)
	})
	return v, err
}

func replaceAccess(ctx context.Context, tx pgx.Tx, clientID int64, poolIDs []int64) error {
	if _, err := tx.Exec(ctx, "DELETE FROM client_model_access WHERE client_id=$1::bigint", clientID); err != nil {
		return errors.New("clear PostgreSQL client access")
	}
	seen := map[int64]bool{}
	for _, id := range poolIDs {
		if seen[id] {
			continue
		}
		seen[id] = true
		if _, err := tx.Exec(ctx, "INSERT INTO client_model_access(client_id,model_pool_id,enabled) VALUES($1::bigint,$2::bigint,true)", clientID, id); err != nil {
			return errors.New("grant PostgreSQL client access")
		}
	}
	return nil
}

func (s *Store) CreatePool(ctx context.Context, p basestore.CreatePoolParams) (domain.ModelPool, error) {
	v := domain.ModelPool{PublicModelName: p.PublicModelName, UpstreamModelName: p.UpstreamModelName, Enabled: p.Enabled, MaxGatewayInflight: p.MaxGatewayInflight, MaxWaiting: p.MaxWaiting, Revision: 1}
	if err := v.Validate(); err != nil {
		return v, err
	}
	now := time.Now().UTC()
	v.CreatedAt = now
	v.UpdatedAt = now
	err := s.mutation(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO model_pools(public_model_name,upstream_model_name,enabled,max_gateway_inflight,max_waiting,created_at,updated_at) VALUES($1::text,$2::text,$3::boolean,$4::integer,$5::integer,$6::timestamptz,$6::timestamptz) RETURNING id`, v.PublicModelName, v.UpstreamModelName, v.Enabled, v.MaxGatewayInflight, v.MaxWaiting, now).Scan(&v.ID); err != nil {
			return errors.New("insert PostgreSQL model pool")
		}
		_, err := tx.Exec(ctx, "INSERT INTO pool_admission_scopes(pool_id,updated_at) VALUES($1::bigint,$2::timestamptz)", v.ID, now)
		return err
	})
	return v, err
}

func (s *Store) UpdatePool(ctx context.Context, id int64, p basestore.UpdatePoolParams) (domain.ModelPool, error) {
	v := domain.ModelPool{ID: id, PublicModelName: p.PublicModelName, UpstreamModelName: p.UpstreamModelName, Enabled: p.Enabled, MaxGatewayInflight: p.MaxGatewayInflight, MaxWaiting: p.MaxWaiting}
	err := s.mutation(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, "SELECT pool_id FROM pool_admission_scopes WHERE pool_id=$1::bigint FOR UPDATE", id); err != nil {
			return err
		}
		var old domain.ModelPool
		if err := tx.QueryRow(ctx, "SELECT max_gateway_inflight,revision,created_at FROM model_pools WHERE id=$1::bigint FOR UPDATE", id).Scan(&old.MaxGatewayInflight, &old.Revision, &v.CreatedAt); err != nil {
			return err
		}
		if err := v.ValidateUpdate(old); err != nil {
			return err
		}
		v.Revision = old.Revision + 1
		v.UpdatedAt = time.Now().UTC()
		tag, err := tx.Exec(ctx, `UPDATE model_pools SET public_model_name=$2::text,upstream_model_name=$3::text,enabled=$4::boolean,max_gateway_inflight=$5::integer,max_waiting=$6::integer,revision=revision+1,updated_at=$7::timestamptz WHERE id=$1::bigint`, id, v.PublicModelName, v.UpstreamModelName, v.Enabled, v.MaxGatewayInflight, v.MaxWaiting, v.UpdatedAt)
		if err != nil || tag.RowsAffected() != 1 {
			return errors.New("update PostgreSQL model pool")
		}
		return nil
	})
	return v, err
}

func (s *Store) CreateBackend(ctx context.Context, p basestore.CreateBackendParams) (domain.Backend, error) {
	v := domain.Backend{ModelPoolID: p.ModelPoolID, Name: p.Name, BaseURL: p.BaseURL, Enabled: p.Enabled, Draining: p.Draining, CapacityHint: p.CapacityHint, RunningSoftLimit: p.RunningSoftLimit, UpstreamAPIKeyEnv: p.UpstreamAPIKeyEnv, Revision: 1}
	if err := v.Validate(); err != nil {
		return v, err
	}
	now := time.Now().UTC()
	v.CreatedAt = now
	v.UpdatedAt = now
	err := s.mutation(ctx, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO backends(model_pool_id,name,base_url,enabled,draining,capacity_hint,running_soft_limit,upstream_api_key_env,created_at,updated_at) VALUES($1::bigint,$2::text,$3::text,$4::boolean,$5::boolean,$6::double precision,$7::double precision,$8::text,$9::timestamptz,$9::timestamptz) RETURNING id`, v.ModelPoolID, v.Name, v.BaseURL, v.Enabled, v.Draining, v.CapacityHint, v.RunningSoftLimit, v.UpstreamAPIKeyEnv, now).Scan(&v.ID); err != nil {
			return errors.New("insert PostgreSQL backend")
		}
		_, err := tx.Exec(ctx, "INSERT INTO backend_circuit_state(backend_id,backend_revision,state,generation,half_open_succeeded,updated_at) VALUES($1::bigint,1,'closed',0,false,$2::timestamptz)", v.ID, now)
		return err
	})
	return v, err
}

func (s *Store) UpdateBackend(ctx context.Context, id int64, p basestore.UpdateBackendParams) (domain.Backend, error) {
	v := domain.Backend{ID: id, ModelPoolID: p.ModelPoolID, Name: p.Name, BaseURL: p.BaseURL, Enabled: p.Enabled, Draining: p.Draining, CapacityHint: p.CapacityHint, RunningSoftLimit: p.RunningSoftLimit, UpstreamAPIKeyEnv: p.UpstreamAPIKeyEnv}
	if err := v.Validate(); err != nil {
		return v, err
	}
	err := s.mutation(ctx, func(tx pgx.Tx) error {
		var oldRev int64
		if _, err := tx.Exec(ctx, "SELECT backend_id FROM backend_circuit_state WHERE backend_id=$1::bigint FOR UPDATE", id); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, "SELECT revision,created_at FROM backends WHERE id=$1::bigint FOR UPDATE", id).Scan(&oldRev, &v.CreatedAt); err != nil {
			return err
		}
		v.Revision = oldRev + 1
		var err error
		v.UpdatedAt, err = supersedeProbes(ctx, tx, id)
		if err != nil {
			return err
		}
		tag, err := tx.Exec(ctx, `UPDATE backends SET model_pool_id=$2::bigint,name=$3::text,base_url=$4::text,enabled=$5::boolean,draining=$6::boolean,capacity_hint=$7::double precision,running_soft_limit=$8::double precision,upstream_api_key_env=$9::text,revision=revision+1,updated_at=$10::timestamptz WHERE id=$1::bigint`, id, v.ModelPoolID, v.Name, v.BaseURL, v.Enabled, v.Draining, v.CapacityHint, v.RunningSoftLimit, v.UpstreamAPIKeyEnv, v.UpdatedAt)
		if err != nil || tag.RowsAffected() != 1 {
			return errors.New("update PostgreSQL backend")
		}
		_, err = tx.Exec(ctx, "UPDATE backend_circuit_state SET backend_revision=$2::bigint,state='closed',generation=generation+1,opened_at=NULL,half_open_succeeded=false,updated_at=$3::timestamptz WHERE backend_id=$1::bigint", id, v.Revision, v.UpdatedAt)
		return err
	})
	return v, err
}

func (s *Store) SetBackendDraining(ctx context.Context, id int64, draining bool) error {
	return s.mutation(ctx, func(tx pgx.Tx) error {
		var rev int64
		if _, err := tx.Exec(ctx, "SELECT backend_id FROM backend_circuit_state WHERE backend_id=$1::bigint FOR UPDATE", id); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, "SELECT revision FROM backends WHERE id=$1::bigint FOR UPDATE", id).Scan(&rev); err != nil {
			return err
		}
		now, err := supersedeProbes(ctx, tx, id)
		if err != nil {
			return err
		}
		_, err = tx.Exec(ctx, "UPDATE backends SET draining=$2::boolean,revision=revision+1,updated_at=$3::timestamptz WHERE id=$1::bigint", id, draining, now)
		if err == nil {
			_, err = tx.Exec(ctx, "UPDATE backend_circuit_state SET backend_revision=$2::bigint,state='closed',generation=generation+1,opened_at=NULL,half_open_succeeded=false,updated_at=$3::timestamptz WHERE backend_id=$1::bigint", id, rev+1, now)
		}
		return err
	})
}

func supersedeProbes(ctx context.Context, tx pgx.Tx, id int64) (time.Time, error) {
	if _, err := tx.Exec(ctx, "SELECT permit_id FROM backend_circuit_probes WHERE backend_id=$1::bigint AND outcome IS NULL ORDER BY permit_id FOR UPDATE", id); err != nil {
		return time.Time{}, err
	}
	var now time.Time
	if err := tx.QueryRow(ctx, "SELECT clock_timestamp()").Scan(&now); err != nil {
		return time.Time{}, err
	}
	_, err := tx.Exec(ctx, `UPDATE backend_circuit_probes SET outcome='superseded',processed_at=$2::timestamptz,retain_until=GREATEST($2::timestamptz,acquisition_started_at)+interval '24 hours' WHERE backend_id=$1::bigint AND outcome IS NULL`, id, now)
	return now, err
}

func (s *Store) CreateAPIKey(ctx context.Context, p basestore.CreateAPIKeyParams) (domain.APIKey, error) {
	v := domain.APIKey{ClientID: p.ClientID, Prefix: p.Prefix, SecretHash: p.SecretHash, ExpiresAt: p.ExpiresAt, CreatedAt: time.Now().UTC()}
	err := s.mutation(ctx, func(tx pgx.Tx) error {
		return tx.QueryRow(ctx, "INSERT INTO api_keys(client_id,prefix,secret_hash,created_at,expires_at) VALUES($1::bigint,$2::text,$3::bytea,$4::timestamptz,$5::timestamptz) RETURNING id", v.ClientID, v.Prefix, v.SecretHash[:], v.CreatedAt, v.ExpiresAt).Scan(&v.ID)
	})
	return v, err
}

func (s *Store) RevokeAPIKey(ctx context.Context, id int64) error {
	return s.mutation(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, "UPDATE api_keys SET revoked_at=clock_timestamp() WHERE id=$1::bigint AND revoked_at IS NULL", id)
		if err != nil || tag.RowsAffected() != 1 {
			return pgx.ErrNoRows
		}
		return nil
	})
}

func (s *Store) TouchKeyLastUsed(ctx context.Context, id int64, at time.Time) error {
	_, err := s.config.Exec(ctx, "UPDATE api_keys SET last_used_at=GREATEST(COALESCE(last_used_at,$2::timestamptz),$2::timestamptz) WHERE id=$1::bigint", id, at.UTC())
	if err != nil {
		return errors.New("update PostgreSQL API key usage")
	}
	return nil
}

func (s *Store) LoadSnapshot(ctx context.Context) (registry.Data, error) {
	tx, err := s.config.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return registry.Data{}, errors.New("begin PostgreSQL configuration snapshot")
	}
	defer tx.Rollback(ctx)
	var out registry.Data
	if err := tx.QueryRow(ctx, "SELECT revision FROM config_meta WHERE singleton=1").Scan(&out.Revision); err != nil {
		return out, err
	}
	if out.Clients, err = pgClients(ctx, tx); err != nil {
		return out, err
	}
	if out.Keys, err = pgKeys(ctx, tx); err != nil {
		return out, err
	}
	if out.Pools, err = pgPools(ctx, tx); err != nil {
		return out, err
	}
	if out.Access, err = pgAccess(ctx, tx); err != nil {
		return out, err
	}
	if out.Backends, err = pgBackends(ctx, tx); err != nil {
		return out, err
	}
	for _, client := range out.Clients {
		if err := client.Validate(); err != nil {
			return registry.Data{}, fmt.Errorf("invalid PostgreSQL client %d: %w", client.ID, err)
		}
	}
	for _, pool := range out.Pools {
		if err := pool.Validate(); err != nil {
			return registry.Data{}, fmt.Errorf("invalid PostgreSQL model pool %d: %w", pool.ID, err)
		}
	}
	for _, backend := range out.Backends {
		if err := backend.Validate(); err != nil {
			return registry.Data{}, fmt.Errorf("invalid PostgreSQL backend %d: %w", backend.ID, err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return registry.Data{}, errors.New("commit PostgreSQL configuration snapshot")
	}
	return out, nil
}

func (s *Store) CurrentRevision(ctx context.Context) (int64, error) {
	var revision int64
	if err := s.config.QueryRow(ctx, "SELECT revision FROM config_meta WHERE singleton=1").Scan(&revision); err != nil {
		return 0, errors.New("read PostgreSQL configuration revision")
	}
	return revision, nil
}

func pgClients(ctx context.Context, q pgx.Tx) ([]domain.Client, error) {
	rows, err := q.Query(ctx, "SELECT id,revision,name,enabled,priority_class,vllm_priority,max_concurrency,requests_per_minute,tokens_per_minute,created_at,updated_at FROM clients ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (v domain.Client, err error) {
		err = r.Scan(&v.ID, &v.Revision, &v.Name, &v.Enabled, &v.PriorityClass, &v.VLLMPriority, &v.MaxConcurrency, &v.RequestsPerMinute, &v.TokensPerMinute, &v.CreatedAt, &v.UpdatedAt)
		return
	})
}
func pgKeys(ctx context.Context, q pgx.Tx) ([]domain.APIKey, error) {
	rows, err := q.Query(ctx, "SELECT id,client_id,prefix,secret_hash,created_at,expires_at,revoked_at,last_used_at FROM api_keys ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (v domain.APIKey, err error) {
		var hash []byte
		err = r.Scan(&v.ID, &v.ClientID, &v.Prefix, &hash, &v.CreatedAt, &v.ExpiresAt, &v.RevokedAt, &v.LastUsedAt)
		copy(v.SecretHash[:], hash)
		if err == nil && len(hash) != len(v.SecretHash) {
			err = fmt.Errorf("API key %d has invalid hash length", v.ID)
		}
		return
	})
}
func pgPools(ctx context.Context, q pgx.Tx) ([]domain.ModelPool, error) {
	rows, err := q.Query(ctx, "SELECT id,revision,public_model_name,upstream_model_name,enabled,max_gateway_inflight,max_waiting,created_at,updated_at FROM model_pools ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (v domain.ModelPool, err error) {
		err = r.Scan(&v.ID, &v.Revision, &v.PublicModelName, &v.UpstreamModelName, &v.Enabled, &v.MaxGatewayInflight, &v.MaxWaiting, &v.CreatedAt, &v.UpdatedAt)
		return
	})
}
func pgAccess(ctx context.Context, q pgx.Tx) ([]domain.ClientModelAccess, error) {
	rows, err := q.Query(ctx, "SELECT client_id,model_pool_id,enabled FROM client_model_access ORDER BY client_id,model_pool_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (v domain.ClientModelAccess, err error) {
		err = r.Scan(&v.ClientID, &v.ModelPoolID, &v.Enabled)
		return
	})
}
func pgBackends(ctx context.Context, q pgx.Tx) ([]domain.Backend, error) {
	rows, err := q.Query(ctx, "SELECT id,revision,model_pool_id,name,base_url,enabled,draining,capacity_hint,running_soft_limit,upstream_api_key_env,created_at,updated_at FROM backends ORDER BY id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (v domain.Backend, err error) {
		err = r.Scan(&v.ID, &v.Revision, &v.ModelPoolID, &v.Name, &v.BaseURL, &v.Enabled, &v.Draining, &v.CapacityHint, &v.RunningSoftLimit, &v.UpstreamAPIKeyEnv, &v.CreatedAt, &v.UpdatedAt)
		return
	})
}
