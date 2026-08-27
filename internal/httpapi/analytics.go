package httpapi

import (
	"context"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/rislanov/vllm-priority-gateway/internal/analytics"
)

const (
	defaultAnalyticsLimit               = 100
	maximumAnalyticsLimit               = 500
	maximumConcurrentAnalyticsCSVSpools = 2
	analyticsCSVDeliveryBufferSize      = 32 << 10
)

var analyticsQueryNames = map[string]struct{}{
	"from": {}, "to": {}, "client_id": {}, "model_pool_id": {},
	"usage_available": {}, "limit": {}, "offset": {},
}

// AnalyticsQuery is the validated query shared by the admin API and web UI.
type AnalyticsQuery struct {
	Filter analytics.Filter
	Limit  int
	Offset int
}

// ParseAnalyticsQuery applies the admin service's clock and the common analytics
// filtering and pagination rules.
func (s *AdminService) ParseAnalyticsQuery(values url.Values) (AnalyticsQuery, error) {
	return parseAnalyticsQuery(values, s.now().UTC())
}

// Analytics returns the aggregate dataset for a validated filter.
func (s *AdminService) Analytics(ctx context.Context, filter analytics.Filter) (analytics.Dataset, error) {
	return s.analytics.Analytics(ctx, filter)
}

// UsageRequests returns a bounded request-ledger page for a validated query.
func (s *AdminService) UsageRequests(ctx context.Context, query AnalyticsQuery) (analytics.RequestPage, error) {
	return s.analytics.UsageRequests(ctx, query.Filter, query.Limit, query.Offset)
}

// AnalyticsDashboard returns the aggregate dataset and request page from one
// store snapshot for server-side rendering.
func (s *AdminService) AnalyticsDashboard(
	ctx context.Context,
	query AnalyticsQuery,
) (analytics.Dataset, analytics.RequestPage, error) {
	return s.analytics.AnalyticsDashboard(ctx, query.Filter, query.Limit, query.Offset)
}

// StreamUsageRequests streams the complete filtered request ledger in stable
// chronological order. Pagination in query is intentionally ignored.
func (s *AdminService) StreamUsageRequests(
	ctx context.Context,
	query AnalyticsQuery,
	yield func(analytics.RequestRecord) error,
) error {
	return s.analytics.StreamUsageRequests(ctx, query.Filter, yield)
}

func parseAnalyticsQuery(values url.Values, now time.Time) (AnalyticsQuery, error) {
	for name := range values {
		if _, ok := analyticsQueryNames[name]; !ok {
			return AnalyticsQuery{}, fmt.Errorf("unsupported analytics query parameter %q", name)
		}
		if len(values[name]) != 1 {
			return AnalyticsQuery{}, fmt.Errorf("analytics query parameter %q must appear once", name)
		}
	}

	query := AnalyticsQuery{
		Filter: analytics.Filter{From: now.Add(-24 * time.Hour), To: now},
		Limit:  defaultAnalyticsLimit,
	}
	fromValue, fromPresent := singleAnalyticsValue(values, "from")
	toValue, toPresent := singleAnalyticsValue(values, "to")
	if fromPresent != toPresent {
		return AnalyticsQuery{}, errors.New("from and to must be supplied together")
	}
	if fromPresent {
		from, err := time.Parse(time.RFC3339, fromValue)
		if err != nil {
			return AnalyticsQuery{}, errors.New("from must be an RFC3339 timestamp")
		}
		to, err := time.Parse(time.RFC3339, toValue)
		if err != nil {
			return AnalyticsQuery{}, errors.New("to must be an RFC3339 timestamp")
		}
		if !from.Before(to) {
			return AnalyticsQuery{}, errors.New("from must be before to")
		}
		query.Filter.From = from.UTC()
		query.Filter.To = to.UTC()
	}

	clientID, err := positiveAnalyticsID(values, "client_id")
	if err != nil {
		return AnalyticsQuery{}, err
	}
	query.Filter.ClientID = clientID
	modelPoolID, err := positiveAnalyticsID(values, "model_pool_id")
	if err != nil {
		return AnalyticsQuery{}, err
	}
	query.Filter.ModelPoolID = modelPoolID

	if value, present := singleAnalyticsValue(values, "usage_available"); present {
		var usageAvailable bool
		switch value {
		case "true":
			usageAvailable = true
		case "false":
			usageAvailable = false
		default:
			return AnalyticsQuery{}, errors.New("usage_available must be exactly true or false")
		}
		query.Filter.UsageAvailable = &usageAvailable
	}
	if value, present := singleAnalyticsValue(values, "limit"); present {
		limit, err := strconv.Atoi(value)
		if err != nil || limit < 1 || limit > maximumAnalyticsLimit {
			return AnalyticsQuery{}, fmt.Errorf("limit must be between 1 and %d", maximumAnalyticsLimit)
		}
		query.Limit = limit
	}
	if value, present := singleAnalyticsValue(values, "offset"); present {
		offset, err := strconv.Atoi(value)
		if err != nil || offset < 0 {
			return AnalyticsQuery{}, errors.New("offset must be a non-negative integer")
		}
		query.Offset = offset
	}
	return query, nil
}

func singleAnalyticsValue(values url.Values, name string) (string, bool) {
	items, ok := values[name]
	if !ok || len(items) != 1 {
		return "", ok
	}
	return items[0], true
}

func positiveAnalyticsID(values url.Values, name string) (*int64, error) {
	value, present := singleAnalyticsValue(values, name)
	if !present {
		return nil, nil
	}
	id, err := strconv.ParseInt(value, 10, 64)
	if err != nil || id <= 0 {
		return nil, fmt.Errorf("%s must be a positive integer", name)
	}
	return &id, nil
}

func analyticsHandler(service *AdminService) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		query, ok := analyticsRequestQuery(writer, request, service)
		if !ok {
			return
		}
		dataset, err := service.Analytics(request.Context(), query.Filter)
		if err != nil {
			writeAnalyticsStoreError(writer, request, err)
			return
		}
		writeAdminJSON(writer, http.StatusOK, dataset)
	}
}

func analyticsRequestsHandler(service *AdminService) http.HandlerFunc {
	return func(writer http.ResponseWriter, request *http.Request) {
		query, ok := analyticsRequestQuery(writer, request, service)
		if !ok {
			return
		}
		page, err := service.UsageRequests(request.Context(), query)
		if err != nil {
			writeAnalyticsStoreError(writer, request, err)
			return
		}
		writeAdminJSON(writer, http.StatusOK, page)
	}
}

func analyticsCSVHandler(service *AdminService) http.HandlerFunc {
	spoolSlots := make(chan struct{}, maximumConcurrentAnalyticsCSVSpools)
	return func(writer http.ResponseWriter, request *http.Request) {
		query, ok := analyticsRequestQuery(writer, request, service)
		if !ok {
			return
		}
		select {
		case spoolSlots <- struct{}{}:
		case <-request.Context().Done():
			return
		default:
			writer.Header().Set("Retry-After", "1")
			writeAdminJSONError(writer, http.StatusServiceUnavailable, "analytics_export_busy", "Analytics export capacity is temporarily exhausted")
			return
		}
		spoolSlotHeld := true
		releaseSpoolSlot := func() {
			if spoolSlotHeld {
				<-spoolSlots
				spoolSlotHeld = false
			}
		}
		defer releaseSpoolSlot()

		spool, err := os.CreateTemp("", "llmgw-usage-analytics-*.csv")
		if err != nil {
			writeAnalyticsExportError(writer, request)
			return
		}
		spoolPath := spool.Name()
		defer func() {
			_ = spool.Close()
			_ = os.Remove(spoolPath)
		}()
		if err := spool.Chmod(0o600); err != nil {
			writeAnalyticsExportError(writer, request)
			return
		}

		csvWriter := csv.NewWriter(spool)
		csvWriter.UseCRLF = true
		if err := csvWriter.Write(analyticsCSVHeader); err != nil {
			writeAnalyticsExportError(writer, request)
			return
		}
		err = service.StreamUsageRequests(request.Context(), query, func(record analytics.RequestRecord) error {
			if err := request.Context().Err(); err != nil {
				return err
			}
			return writeAnalyticsCSVRecord(csvWriter, record)
		})
		if err != nil {
			writeAnalyticsStoreError(writer, request, err)
			return
		}
		csvWriter.Flush()
		if err := csvWriter.Error(); err != nil {
			writeAnalyticsExportError(writer, request)
			return
		}
		if _, err := spool.Seek(0, io.SeekStart); err != nil {
			writeAnalyticsExportError(writer, request)
			return
		}
		releaseSpoolSlot()

		writer.Header().Set("Content-Type", "text/csv; charset=utf-8")
		writer.Header().Set("Content-Disposition", `attachment; filename="usage-analytics.csv"`)
		writer.WriteHeader(http.StatusOK)
		_ = copyAnalyticsCSV(request.Context(), writer, spool)
	}
}

func analyticsRequestQuery(writer http.ResponseWriter, request *http.Request, service *AdminService) (AnalyticsQuery, bool) {
	query, err := service.ParseAnalyticsQuery(request.URL.Query())
	if err != nil {
		writeAdminJSONError(writer, http.StatusBadRequest, "invalid_analytics_query", err.Error())
		return AnalyticsQuery{}, false
	}
	return query, true
}

func writeAnalyticsStoreError(writer http.ResponseWriter, request *http.Request, err error) {
	if request.Context().Err() != nil || errors.Is(err, context.Canceled) {
		return
	}
	writeAdminJSONError(writer, http.StatusInternalServerError, "analytics_query_failed", "Unable to query analytics")
}

func writeAnalyticsExportError(writer http.ResponseWriter, request *http.Request) {
	if request.Context().Err() != nil {
		return
	}
	writeAdminJSONError(writer, http.StatusInternalServerError, "analytics_export_failed", "Unable to prepare analytics export")
}

var analyticsCSVHeader = []string{
	"id", "occurred_at", "request_id", "parent_request_id", "client_id", "client_name", "model_pool_id", "model_name",
	"backend_name", "http_status", "duration_ms", "ttft_ms", "retry_count", "disconnected", "usage_available",
	"input_tokens", "output_tokens", "cache_read_tokens",
}

func writeAnalyticsCSVRecord(csvWriter *csv.Writer, record analytics.RequestRecord) error {
	row := []string{
		strconv.FormatInt(record.ID, 10),
		record.OccurredAt.UTC().Format(time.RFC3339Nano),
		neutralizeCSVFormula(record.RequestID),
		neutralizeCSVFormula(record.ParentRequestID),
		strconv.FormatInt(record.ClientID, 10),
		neutralizeCSVFormula(record.ClientName),
		strconv.FormatInt(record.ModelPoolID, 10),
		neutralizeCSVFormula(record.ModelName),
		neutralizeCSVFormula(record.BackendName),
		strconv.Itoa(record.HTTPStatus),
		strconv.FormatInt(record.DurationMS, 10),
		nullableCSVInt(record.TTFTMS),
		strconv.Itoa(record.RetryCount),
		strconv.FormatBool(record.Disconnected),
		strconv.FormatBool(record.UsageAvailable),
		nullableCSVInt(record.InputTokens),
		nullableCSVInt(record.OutputTokens),
		nullableCSVInt(record.CacheReadTokens),
	}
	return csvWriter.Write(row)
}

func copyAnalyticsCSV(ctx context.Context, writer http.ResponseWriter, source io.Reader) error {
	buffer := make([]byte, analyticsCSVDeliveryBufferSize)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		read, readErr := source.Read(buffer)
		if read > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			written, writeErr := writer.Write(buffer[:read])
			if writeErr != nil {
				return writeErr
			}
			if written != read {
				return io.ErrShortWrite
			}
			if flusher, ok := writer.(http.Flusher); ok {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return nil
			}
			return readErr
		}
	}
}

func neutralizeCSVFormula(value string) string {
	runeValue, _ := utf8.DecodeRuneInString(value)
	switch runeValue {
	case '=', '+', '-', '@':
		return "'" + value
	default:
		return value
	}
}

func nullableCSVInt(value *int64) string {
	if value == nil {
		return ""
	}
	return strconv.FormatInt(*value, 10)
}
