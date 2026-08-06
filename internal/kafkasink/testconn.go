package kafkasink

import (
	"fmt"

	"github.com/IBM/sarama"
)

// TestConnection opens a throwaway client against brokers and confirms the
// topic is reachable and exists - it never publishes a message, matching
// the non-destructive Test button pattern used by internal/nifiavro and
// internal/druid's own TestConnection functions.
func TestConnection(brokers []string, topic string) (string, error) {
	if len(brokers) == 0 {
		return "", fmt.Errorf("kafkasink: no brokers given")
	}
	if topic == "" {
		return "", fmt.Errorf("kafkasink: no topic given")
	}
	client, err := sarama.NewClient(brokers, sarama.NewConfig())
	if err != nil {
		return "", fmt.Errorf("kafkasink: connecting to brokers %v: %w", brokers, err)
	}
	defer client.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		return "", fmt.Errorf("kafkasink: topic %q not reachable: %w", topic, err)
	}
	return fmt.Sprintf("connected, topic %q has %d partition(s)", topic, len(partitions)), nil
}
