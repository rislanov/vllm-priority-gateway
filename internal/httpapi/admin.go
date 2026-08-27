package httpapi

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rislanov/vllm-priority-gateway/internal/apikey"
	"github.com/rislanov/vllm-priority-gateway/internal/domain"
	"github.com/rislanov/vllm-priority-gateway/internal/registry"
	"github.com/rislanov/vllm-priority-gateway/internal/store"
)

type AdminStore interface {
	CreateClient(context.Context, store.CreateClientParams) (domain.Client, error)
	UpdateClient(context.Context, int64, store.UpdateClientParams) (domain.Client, error)
	CreateAPIKey(context.Context, store.CreateAPIKeyParams) (domain.APIKey, error)
	RevokeAPIKey(context.Context, int64) error
	CreatePool(context.Context, store.CreatePoolParams) (domain.ModelPool, error)
	UpdatePool(context.Context, int64, store.UpdatePoolParams) (domain.ModelPool, error)
	CreateBackend(context.Context, store.CreateBackendParams) (domain.Backend, error)
	UpdateBackend(context.Context, int64, store.UpdateBackendParams) (domain.Backend, error)
	SetBackendDraining(context.Context, int64, bool) error
}

type AdminRegistry interface {
	Reload(context.Context) error
	Snapshot() *registry.Snapshot
}

type keyRevocationOverlay interface {
	MarkKeyRevoked(id int64, at time.Time) bool
}

type AdminRuntime interface {
	Reconcile([]domain.Backend) error
	Snapshot(int64, time.Time) domain.BackendRuntime
	PoolSnapshot(int64, time.Time) domain.PoolRuntime
}

type AdminDependencies struct {
	Store      AdminStore
	Registry   AdminRegistry
	Runtime    AdminRuntime
	HMACSecret []byte
	Random     io.Reader
	Now        func() time.Time
}

type AdminService struct {
	store      AdminStore
	registry   AdminRegistry
	runtime    AdminRuntime
	hmacSecret []byte
	random     io.Reader
	now        func() time.Time

	randomMu sync.Mutex
	stateMu  sync.RWMutex
	degraded string
}

func NewAdminService(dependencies AdminDependencies) (*AdminService, error) {
	if dependencies.Store == nil || dependencies.Registry == nil || dependencies.Runtime == nil {
		return nil, errors.New("admin store, registry, and runtime are required")
	}
	if len(dependencies.HMACSecret) < 32 {
		return nil, errors.New("admin API-key HMAC secret must contain at least 32 bytes")
	}
	randomSource := dependencies.Random
	if randomSource == nil {
		randomSource = rand.Reader
	}
	now := dependencies.Now
	if now == nil {
		now = time.Now
	}
	return &AdminService{
		store: dependencies.Store, registry: dependencies.Registry, runtime: dependencies.Runtime,
		hmacSecret: append([]byte(nil), dependencies.HMACSecret...), random: randomSource, now: now,
	}, nil
}

type ClientInput struct {
	Name           string               `json:"name"`
	Enabled        bool                 `json:"enabled"`
	PriorityClass  domain.PriorityClass `json:"priorityClass"`
	VLLMPriority   int                  `json:"vllmPriority"`
	MaxConcurrency int                  `json:"maxConcurrency"`
	ModelPoolIDs   []int64              `json:"modelPoolIds"`
}

type PoolInput struct {
	PublicModelName    string `json:"publicModelName"`
	UpstreamModelName  string `json:"upstreamModelName"`
	Enabled            bool   `json:"enabled"`
	MaxGatewayInflight int    `json:"maxGatewayInflight"`
	MaxWaiting         int    `json:"maxWaiting"`
}

type BackendInput struct {
	ModelPoolID       int64   `json:"modelPoolId"`
	Name              string  `json:"name"`
	BaseURL           string  `json:"baseUrl"`
	Enabled           bool    `json:"enabled"`
	Draining          bool    `json:"draining"`
	CapacityHint      float64 `json:"capacityHint"`
	RunningSoftLimit  float64 `json:"runningSoftLimit"`
	UpstreamAPIKeyEnv string  `json:"upstreamApiKeyEnv"`
}

type KeyInput struct {
	ExpiresAt *time.Time `json:"expiresAt"`
}

type AdminClient struct {
	ID             int64                `json:"id"`
	Name           string               `json:"name"`
	Enabled        bool                 `json:"enabled"`
	PriorityClass  domain.PriorityClass `json:"priorityClass"`
	VLLMPriority   int                  `json:"vllmPriority"`
	MaxConcurrency int                  `json:"maxConcurrency"`
	ModelPoolIDs   []int64              `json:"modelPoolIds"`
	Models         []string             `json:"models"`
}

type AdminKey struct {
	ID         int64      `json:"id"`
	ClientID   int64      `json:"clientId"`
	Client     string     `json:"client"`
	Prefix     string     `json:"prefix"`
	CreatedAt  time.Time  `json:"createdAt"`
	ExpiresAt  *time.Time `json:"expiresAt,omitempty"`
	RevokedAt  *time.Time `json:"revokedAt,omitempty"`
	LastUsedAt *time.Time `json:"lastUsedAt,omitempty"`
	Status     string     `json:"status"`
}

type AdminPool struct {
	ID                 int64              `json:"id"`
	PublicModelName    string             `json:"publicModelName"`
	UpstreamModelName  string             `json:"upstreamModelName"`
	Enabled            bool               `json:"enabled"`
	MaxGatewayInflight int                `json:"maxGatewayInflight"`
	MaxWaiting         int                `json:"maxWaiting"`
	Runtime            domain.PoolRuntime `json:"runtime"`
}

type AdminBackend struct {
	ID                int64                 `json:"id"`
	ModelPoolID       int64                 `json:"modelPoolId"`
	ModelPool         string                `json:"modelPool"`
	Name              string                `json:"name"`
	BaseURL           string                `json:"baseUrl"`
	Enabled           bool                  `json:"enabled"`
	Draining          bool                  `json:"draining"`
	CapacityHint      float64               `json:"capacityHint"`
	RunningSoftLimit  float64               `json:"runningSoftLimit"`
	UpstreamAPIKeyEnv string                `json:"upstreamApiKeyEnv,omitempty"`
	Runtime           domain.BackendRuntime `json:"runtime"`
}

type AdminView struct {
	Revision int64          `json:"revision"`
	Degraded string         `json:"degraded,omitempty"`
	Clients  []AdminClient  `json:"clients"`
	Keys     []AdminKey     `json:"keys"`
	Pools    []AdminPool    `json:"pools"`
	Backends []AdminBackend `json:"backends"`
}

type CreatedKey struct {
	AdminKey
	Secret string `json:"secret"`
}

func (s *AdminService) View() AdminView {
	snapshot := s.registry.Snapshot()
	at := s.now().UTC()
	view := AdminView{Revision: snapshot.Revision}
	s.stateMu.RLock()
	view.Degraded = s.degraded
	s.stateMu.RUnlock()

	for _, client := range snapshot.Clients {
		item := AdminClient{
			ID: client.ID, Name: client.Name, Enabled: client.Enabled, PriorityClass: client.PriorityClass,
			VLLMPriority: client.VLLMPriority, MaxConcurrency: client.MaxConcurrency,
		}
		for poolID, allowed := range snapshot.Access[client.ID] {
			if !allowed {
				continue
			}
			item.ModelPoolIDs = append(item.ModelPoolIDs, poolID)
			if pool, ok := snapshot.PoolsByID[poolID]; ok {
				item.Models = append(item.Models, pool.PublicModelName)
			}
		}
		sort.Slice(item.ModelPoolIDs, func(i, j int) bool { return item.ModelPoolIDs[i] < item.ModelPoolIDs[j] })
		sort.Strings(item.Models)
		view.Clients = append(view.Clients, item)
	}
	for _, candidates := range snapshot.KeyCandidates {
		for _, key := range candidates {
			status := "active"
			if key.RevokedAt != nil {
				status = "revoked"
			} else if key.ExpiresAt != nil && !key.ExpiresAt.After(at) {
				status = "expired"
			}
			client := snapshot.Clients[key.ClientID]
			view.Keys = append(view.Keys, AdminKey{
				ID: key.ID, ClientID: key.ClientID, Client: client.Name, Prefix: key.Prefix,
				CreatedAt: key.CreatedAt, ExpiresAt: key.ExpiresAt, RevokedAt: key.RevokedAt,
				LastUsedAt: key.LastUsedAt, Status: status,
			})
		}
	}
	for _, pool := range snapshot.PoolsByID {
		view.Pools = append(view.Pools, AdminPool{
			ID: pool.ID, PublicModelName: pool.PublicModelName, UpstreamModelName: pool.UpstreamModelName,
			Enabled: pool.Enabled, MaxGatewayInflight: pool.MaxGatewayInflight, MaxWaiting: pool.MaxWaiting,
			Runtime: s.runtime.PoolSnapshot(pool.ID, at),
		})
	}
	for _, backend := range snapshot.BackendsByID {
		pool := snapshot.PoolsByID[backend.ModelPoolID]
		view.Backends = append(view.Backends, AdminBackend{
			ID: backend.ID, ModelPoolID: backend.ModelPoolID, ModelPool: pool.PublicModelName,
			Name: backend.Name, BaseURL: backend.BaseURL, Enabled: backend.Enabled, Draining: backend.Draining,
			CapacityHint: backend.CapacityHint, RunningSoftLimit: backend.RunningSoftLimit,
			UpstreamAPIKeyEnv: backend.UpstreamAPIKeyEnv, Runtime: s.runtime.Snapshot(backend.ID, at),
		})
	}
	sort.Slice(view.Clients, func(i, j int) bool { return view.Clients[i].Name < view.Clients[j].Name })
	sort.Slice(view.Keys, func(i, j int) bool { return view.Keys[i].ID < view.Keys[j].ID })
	sort.Slice(view.Pools, func(i, j int) bool { return view.Pools[i].PublicModelName < view.Pools[j].PublicModelName })
	sort.Slice(view.Backends, func(i, j int) bool { return view.Backends[i].Name < view.Backends[j].Name })
	return view
}

func (s *AdminService) CreateClient(ctx context.Context, input ClientInput) (AdminClient, error) {
	created, err := s.store.CreateClient(ctx, store.CreateClientParams(input))
	if err != nil {
		return AdminClient{}, err
	}
	if err := s.publish(ctx); err != nil {
		return AdminClient{}, err
	}
	return s.client(created.ID), nil
}

func (s *AdminService) UpdateClient(ctx context.Context, id int64, input ClientInput) (AdminClient, error) {
	updated, err := s.store.UpdateClient(ctx, id, store.UpdateClientParams(input))
	if err != nil {
		return AdminClient{}, err
	}
	if err := s.publish(ctx); err != nil {
		return AdminClient{}, err
	}
	return s.client(updated.ID), nil
}

func (s *AdminService) CreateKey(ctx context.Context, clientID int64, input KeyInput) (CreatedKey, error) {
	s.randomMu.Lock()
	plain, err := apikey.Generate(s.random)
	s.randomMu.Unlock()
	if err != nil {
		return CreatedKey{}, err
	}
	key, err := s.store.CreateAPIKey(ctx, store.CreateAPIKeyParams{
		ClientID: clientID, Prefix: plain.Prefix, SecretHash: apikey.Digest(s.hmacSecret, plain.Value), ExpiresAt: input.ExpiresAt,
	})
	if err != nil {
		return CreatedKey{}, err
	}
	if err := s.publish(ctx); err != nil {
		return CreatedKey{}, err
	}
	client := s.registry.Snapshot().Clients[clientID]
	return CreatedKey{AdminKey: AdminKey{
		ID: key.ID, ClientID: key.ClientID, Client: client.Name, Prefix: key.Prefix,
		CreatedAt: key.CreatedAt, ExpiresAt: key.ExpiresAt, Status: "active",
	}, Secret: plain.Value}, nil
}

func (s *AdminService) RevokeKey(ctx context.Context, id int64) error {
	if err := s.store.RevokeAPIKey(ctx, id); err != nil {
		return err
	}
	if overlay, ok := s.registry.(keyRevocationOverlay); ok {
		overlay.MarkKeyRevoked(id, s.now().UTC())
	}
	return s.publish(ctx)
}

func (s *AdminService) CreatePool(ctx context.Context, input PoolInput) (AdminPool, error) {
	pool, err := s.store.CreatePool(ctx, store.CreatePoolParams(input))
	if err != nil {
		return AdminPool{}, err
	}
	if err := s.publish(ctx); err != nil {
		return AdminPool{}, err
	}
	return s.pool(pool.ID), nil
}

func (s *AdminService) UpdatePool(ctx context.Context, id int64, input PoolInput) (AdminPool, error) {
	pool, err := s.store.UpdatePool(ctx, id, store.UpdatePoolParams(input))
	if err != nil {
		return AdminPool{}, err
	}
	if err := s.publish(ctx); err != nil {
		return AdminPool{}, err
	}
	return s.pool(pool.ID), nil
}

func (s *AdminService) CreateBackend(ctx context.Context, input BackendInput) (AdminBackend, error) {
	backend, err := s.store.CreateBackend(ctx, store.CreateBackendParams(input))
	if err != nil {
		return AdminBackend{}, err
	}
	if err := s.publish(ctx); err != nil {
		return AdminBackend{}, err
	}
	return s.backend(backend.ID), nil
}

func (s *AdminService) UpdateBackend(ctx context.Context, id int64, input BackendInput) (AdminBackend, error) {
	backend, err := s.store.UpdateBackend(ctx, id, store.UpdateBackendParams(input))
	if err != nil {
		return AdminBackend{}, err
	}
	if err := s.publish(ctx); err != nil {
		return AdminBackend{}, err
	}
	return s.backend(backend.ID), nil
}

func (s *AdminService) SetBackendDraining(ctx context.Context, id int64, draining bool) (AdminBackend, error) {
	if err := s.store.SetBackendDraining(ctx, id, draining); err != nil {
		return AdminBackend{}, err
	}
	if err := s.publish(ctx); err != nil {
		return AdminBackend{}, err
	}
	return s.backend(id), nil
}

func (s *AdminService) publish(ctx context.Context) error {
	publishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := s.registry.Reload(publishCtx); err != nil {
		s.setDegraded(err)
		return fmt.Errorf("publish configuration: %w", err)
	}
	snapshot := s.registry.Snapshot()
	backends := make([]domain.Backend, 0, len(snapshot.BackendsByID))
	for _, backend := range snapshot.BackendsByID {
		backends = append(backends, backend)
	}
	if err := s.runtime.Reconcile(backends); err != nil {
		s.setDegraded(err)
		return fmt.Errorf("reconcile backend monitors: %w", err)
	}
	s.setDegraded(nil)
	return nil
}

func (s *AdminService) setDegraded(err error) {
	s.stateMu.Lock()
	if err == nil {
		s.degraded = ""
	} else {
		s.degraded = err.Error()
	}
	s.stateMu.Unlock()
}

func (s *AdminService) client(id int64) AdminClient {
	for _, client := range s.View().Clients {
		if client.ID == id {
			return client
		}
	}
	return AdminClient{}
}

func (s *AdminService) pool(id int64) AdminPool {
	for _, pool := range s.View().Pools {
		if pool.ID == id {
			return pool
		}
	}
	return AdminPool{}
}

func (s *AdminService) backend(id int64) AdminBackend {
	for _, backend := range s.View().Backends {
		if backend.ID == id {
			return backend
		}
	}
	return AdminBackend{}
}

func NewAdminAPI(service *AdminService) http.Handler {
	router := chi.NewRouter()
	router.Route("/admin/api", func(router chi.Router) {
		router.Get("/clients", func(writer http.ResponseWriter, _ *http.Request) {
			writeAdminJSON(writer, http.StatusOK, map[string]any{"revision": service.View().Revision, "clients": service.View().Clients})
		})
		router.Post("/clients", createClientHandler(service))
		router.Put("/clients/{id}", updateClientHandler(service))
		router.Post("/clients/{id}/keys", createKeyHandler(service))
		router.Delete("/keys/{id}", revokeKeyHandler(service))
		router.Get("/pools", func(writer http.ResponseWriter, _ *http.Request) {
			view := service.View()
			writeAdminJSON(writer, http.StatusOK, map[string]any{"revision": view.Revision, "pools": view.Pools})
		})
		router.Post("/pools", createPoolHandler(service))
		router.Put("/pools/{id}", updatePoolHandler(service))
		router.Get("/backends", func(writer http.ResponseWriter, _ *http.Request) {
			view := service.View()
			writeAdminJSON(writer, http.StatusOK, map[string]any{"revision": view.Revision, "backends": view.Backends})
		})
		router.Post("/backends", createBackendHandler(service))
		router.Put("/backends/{id}", updateBackendHandler(service))
		router.Post("/backends/{id}/drain", drainHandler(service, true))
		router.Post("/backends/{id}/resume", drainHandler(service, false))
		router.Get("/status", func(writer http.ResponseWriter, _ *http.Request) {
			writeAdminJSON(writer, http.StatusOK, service.View())
		})
	})
	return router
}

func createClientHandler(service *AdminService) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var input ClientInput
		if !decodeAdminJSON(writer, request, &input) {
			return
		}
		value, err := service.CreateClient(request.Context(), input)
		writeAdminResult(writer, http.StatusCreated, value, err)
	}
}

func updateClientHandler(service *AdminService) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		id, ok := adminID(writer, request)
		if !ok {
			return
		}
		var input ClientInput
		if !decodeAdminJSON(writer, request, &input) {
			return
		}
		value, err := service.UpdateClient(request.Context(), id, input)
		writeAdminResult(writer, http.StatusOK, value, err)
	}
}

func createKeyHandler(service *AdminService) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		id, ok := adminID(writer, request)
		if !ok {
			return
		}
		var input KeyInput
		if !decodeAdminJSON(writer, request, &input) {
			return
		}
		value, err := service.CreateKey(request.Context(), id, input)
		writeAdminResult(writer, http.StatusCreated, value, err)
	}
}

func revokeKeyHandler(service *AdminService) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		id, ok := adminID(writer, request)
		if !ok {
			return
		}
		if err := service.RevokeKey(request.Context(), id); err != nil {
			writeAdminError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}
}

func createPoolHandler(service *AdminService) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var input PoolInput
		if !decodeAdminJSON(writer, request, &input) {
			return
		}
		value, err := service.CreatePool(request.Context(), input)
		writeAdminResult(writer, http.StatusCreated, value, err)
	}
}

func updatePoolHandler(service *AdminService) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		id, ok := adminID(writer, request)
		if !ok {
			return
		}
		var input PoolInput
		if !decodeAdminJSON(writer, request, &input) {
			return
		}
		value, err := service.UpdatePool(request.Context(), id, input)
		writeAdminResult(writer, http.StatusOK, value, err)
	}
}

func createBackendHandler(service *AdminService) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		var input BackendInput
		if !decodeAdminJSON(writer, request, &input) {
			return
		}
		value, err := service.CreateBackend(request.Context(), input)
		writeAdminResult(writer, http.StatusCreated, value, err)
	}
}

func updateBackendHandler(service *AdminService) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		id, ok := adminID(writer, request)
		if !ok {
			return
		}
		var input BackendInput
		if !decodeAdminJSON(writer, request, &input) {
			return
		}
		value, err := service.UpdateBackend(request.Context(), id, input)
		writeAdminResult(writer, http.StatusOK, value, err)
	}
}

func drainHandler(service *AdminService, draining bool) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		id, ok := adminID(writer, request)
		if !ok {
			return
		}
		value, err := service.SetBackendDraining(request.Context(), id, draining)
		writeAdminResult(writer, http.StatusOK, value, err)
	}
}

func decodeAdminJSON(writer http.ResponseWriter, request *http.Request, output any) bool {
	request.Body = http.MaxBytesReader(writer, request.Body, 1<<20)
	decoder := json.NewDecoder(request.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(output); err != nil {
		writeAdminJSONError(writer, http.StatusBadRequest, "invalid_json", "Request body must be valid JSON")
		return false
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeAdminJSONError(writer, http.StatusBadRequest, "invalid_json", "Request body must contain one JSON value")
		return false
	}
	return true
}

func adminID(writer http.ResponseWriter, request *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(chi.URLParam(request, "id"), 10, 64)
	if err != nil || id <= 0 {
		writeAdminJSONError(writer, http.StatusBadRequest, "invalid_id", "Resource ID must be a positive integer")
		return 0, false
	}
	return id, true
}

func writeAdminResult(writer http.ResponseWriter, status int, value any, err error) {
	if err != nil {
		writeAdminError(writer, err)
		return
	}
	writeAdminJSON(writer, status, value)
}

func writeAdminError(writer http.ResponseWriter, err error) {
	message := err.Error()
	status, code := http.StatusBadRequest, "validation_error"
	switch {
	case errors.Is(err, sql.ErrNoRows):
		status, code = http.StatusNotFound, "not_found"
	case strings.Contains(message, "UNIQUE constraint failed"), strings.Contains(message, "FOREIGN KEY constraint failed"):
		status, code = http.StatusConflict, "conflict"
	case strings.Contains(message, "publish configuration"), strings.Contains(message, "reconcile backend monitors"):
		status, code = http.StatusServiceUnavailable, "configuration_degraded"
	}
	writeAdminJSONError(writer, status, code, message)
}

func writeAdminJSONError(writer http.ResponseWriter, status int, code, message string) {
	writeAdminJSON(writer, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func writeAdminJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json; charset=utf-8")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
