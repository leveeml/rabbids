package rabbids

import (
	"testing"
	"time"
)

func Test_delayDelivery_CalculateRoutingKey(t *testing.T) {
	tests := []struct {
		name      string
		delay     time.Duration
		address   string
		wantTopic string
		wantEx    string
	}{
		{
			name:      "one minute",
			delay:     time.Minute,
			address:   "test",
			wantTopic: "0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.1.1.1.1.0.0.test",
			wantEx:    "rabbids.delay-level-5",
		},
		{
			name:      "10 seconds",
			delay:     10 * time.Second,
			address:   "test",
			wantTopic: "0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.0.1.0.1.0.test",
			wantEx:    "rabbids.delay-level-3",
		},
		{
			name:      "max value",
			delay:     268435455 * time.Second,
			address:   "test",
			wantTopic: "1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.test",
			wantEx:    "rabbids.delay-level-27",
		},
		{
			name:      "above the max value",
			delay:     368435455 * time.Second,
			address:   "test",
			wantTopic: "1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.1.test",
			wantEx:    "rabbids.delay-level-27",
		},
		{
			name:      "12 days",
			delay:     12 * 24 * time.Hour,
			address:   "test",
			wantTopic: "0.0.0.0.0.0.0.0.1.1.1.1.1.1.0.1.0.0.1.0.0.0.0.0.0.0.0.0.test",
			wantEx:    "rabbids.delay-level-19",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			d := &delayDelivery{}
			topic, ex := d.CalculateRoutingKey(tt.delay, tt.address)
			if topic != tt.wantTopic {
				t.Errorf("returned wrong topic = %v, want %v", topic, tt.wantTopic)
			}
			if ex != tt.wantEx {
				t.Errorf("returned wrong exchange = %v, want %v", ex, tt.wantEx)
			}
		})
	}
}
