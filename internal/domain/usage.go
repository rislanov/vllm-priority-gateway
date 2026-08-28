package domain

type TokenUsage struct {
	InputTokens     int64
	OutputTokens    int64
	CacheReadTokens *int64
}
