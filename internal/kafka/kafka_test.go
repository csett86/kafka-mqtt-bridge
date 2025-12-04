package kafka

import (
	"errors"
	"io"
	"net"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "EOF error",
			err:      io.EOF,
			expected: true,
		},
		{
			name:     "unexpected EOF error",
			err:      io.ErrUnexpectedEOF,
			expected: true,
		},
		{
			name:     "wrapped EOF error - not recognized",
			err:      errors.New("connection reset: " + io.EOF.Error()),
			expected: false, // Not wrapped with errors.Is, just string
		},
		{
			name:     "Kafka LeaderNotAvailable - transient",
			err:      kafka.LeaderNotAvailable,
			expected: true,
		},
		{
			name:     "Kafka NotLeaderForPartition - transient",
			err:      kafka.NotLeaderForPartition,
			expected: true,
		},
		{
			name:     "Kafka RequestTimedOut - transient",
			err:      kafka.RequestTimedOut,
			expected: true,
		},
		{
			name:     "Kafka BrokerNotAvailable - non-transient per kafka-go",
			err:      kafka.BrokerNotAvailable,
			expected: false, // kafka.BrokerNotAvailable.Temporary() returns false
		},
		{
			name:     "Kafka ReplicaNotAvailable - non-transient per kafka-go",
			err:      kafka.ReplicaNotAvailable,
			expected: false, // kafka.ReplicaNotAvailable.Temporary() returns false
		},
		{
			name:     "Kafka NetworkException - transient",
			err:      kafka.NetworkException,
			expected: true,
		},
		{
			name:     "Kafka GroupLoadInProgress - transient",
			err:      kafka.GroupLoadInProgress,
			expected: true,
		},
		{
			name:     "Kafka GroupCoordinatorNotAvailable - transient",
			err:      kafka.GroupCoordinatorNotAvailable,
			expected: true,
		},
		{
			name:     "Kafka NotCoordinatorForGroup - transient",
			err:      kafka.NotCoordinatorForGroup,
			expected: true,
		},
		{
			name:     "Kafka NotEnoughReplicas - transient",
			err:      kafka.NotEnoughReplicas,
			expected: true,
		},
		{
			name:     "Kafka NotEnoughReplicasAfterAppend - transient",
			err:      kafka.NotEnoughReplicasAfterAppend,
			expected: true,
		},
		{
			name:     "Kafka RebalanceInProgress - non-transient per kafka-go",
			err:      kafka.RebalanceInProgress,
			expected: false, // kafka.RebalanceInProgress.Temporary() returns false
		},
		{
			name:     "Kafka KafkaStorageError - transient",
			err:      kafka.KafkaStorageError,
			expected: true,
		},
		{
			name:     "Kafka InvalidMessage - explicitly excluded despite Temporary()",
			err:      kafka.InvalidMessage,
			expected: false, // We explicitly exclude this as it indicates corrupt message
		},
		{
			name:     "Kafka TopicAuthorizationFailed - non-transient",
			err:      kafka.TopicAuthorizationFailed,
			expected: false,
		},
		{
			name:     "Kafka UnknownTopicOrPartition - transient per kafka-go",
			err:      kafka.UnknownTopicOrPartition,
			expected: true, // kafka.UnknownTopicOrPartition.Temporary() returns true
		},
		{
			name:     "generic error",
			err:      errors.New("some random error"),
			expected: false,
		},
		{
			name:     "net.OpError",
			err:      &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isTransientError(tt.err)
			if result != tt.expected {
				t.Errorf("isTransientError(%v) = %v, want %v", tt.err, result, tt.expected)
			}
		})
	}
}
