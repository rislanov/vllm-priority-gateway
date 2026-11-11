package domain

import "testing"

func TestResolveRequestSizePolicy(t *testing.T) {
	pool := ModelPool{RequestSizeAdmission: RequestSizePolicy{
		Enabled: true, MaxOutstandingTokens: 32768, EstimationProfile: "qualified-v1",
		Limits: RequestSizeLimits{MaxInputTokens: 8192, MaxOutputTokens: 2048, DefaultOutputTokens: 512},
		ClassLimits: ClassRequestSizeLimits{Background: RequestSizeLimits{
			MaxInputTokens: 2048, MaxOutputTokens: 256, DefaultOutputTokens: 128,
		}},
	}}
	client := Client{PriorityClass: PriorityBackground, RequestSizeLimits: RequestSizeLimits{MaxOutputTokens: 64}}
	for _, tc := range []struct {
		name     string
		override RequestSizeLimits
		want     RequestSizeLimits
	}{
		{"clamp class default to client maximum", client.RequestSizeLimits, RequestSizeLimits{2048, 64, 64}},
		{"prefer client default", RequestSizeLimits{MaxOutputTokens: 64, DefaultOutputTokens: 32}, RequestSizeLimits{2048, 64, 32}},
		{"inherit zeros", RequestSizeLimits{}, RequestSizeLimits{2048, 256, 128}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client.RequestSizeLimits = tc.override
			got, err := ResolveRequestSizePolicy(client, pool)
			if err != nil || !got.Enabled || got.Limits != tc.want || got.EmergencyPoolBudget != 32768 {
				t.Fatalf("resolved policy = %+v, err = %v; want limits %+v and inherited emergency budget", got, err, tc.want)
			}
		})
	}
	for _, value := range []int64{-1, 1_000_000_000_001} {
		client.RequestSizeLimits = RequestSizeLimits{MaxInputTokens: value}
		if _, err := ResolveRequestSizePolicy(client, pool); err == nil {
			t.Fatalf("accepted max input %d", value)
		}
	}
}
