package rabbids

import (
	"testing"
	"time"
)

func Test_calculateRoutingKey(t *testing.T) {
	t.Parallel()

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
			topic, ex := calculateRoutingKey(tt.delay, tt.address)
			if topic != tt.wantTopic {
				t.Errorf("returned wrong topic = %v, want %v", topic, tt.wantTopic)
			}
			if ex != tt.wantEx {
				t.Errorf("returned wrong exchange = %v, want %v", ex, tt.wantEx)
			}
		})
	}
}

func Test_getQueueFromRoutingKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		key  string
		want string
	}{
		{
			name: "single word",
			key:  "0.0.0.0.0.0.0.0.1.1.1.1.1.1.0.1.0.0.1.0.0.0.0.0.0.0.0.0.test",
			want: "test",
		},
		{
			name: "2 words separated by dot",
			key:  "0.0.0.0.0.0.0.0.1.1.1.1.1.1.0.1.0.0.1.0.0.0.0.0.0.0.0.0.test.foo",
			want: "test.foo",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := getQueueFromRoutingKey(tt.key); got != tt.want {
				t.Errorf("getQueueFromRoutingKey() = %v, want %v", got, tt.want)
			}
		})
	}
}
