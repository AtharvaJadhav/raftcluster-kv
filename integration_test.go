package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"raftcluster-kv/raft"
)

func TestIntegration(t *testing.T) {
	// Set up 3 ShardedNodes on different ports
	nodeIDs := []string{"node1", "node2", "node3"}
	ports := []string{"8080", "8081", "8082"}

	// Create ShardedNodes
	nodes := make([]*raft.ShardedNode, 3)
	for i := 0; i < 3; i++ {
		nodes[i] = raft.NewShardedNode(nodeIDs[i], nodeIDs)
		nodes[i].StartHTTPServer(ports[i])
	}

	// Give time for servers to start
	time.Sleep(1 * time.Second)

	// Manually set states for testing - make each node leader for one shard
	for i := 0; i < 3; i++ {
		shard := nodes[i].GetShard(i)
		if shard != nil {
			shard.SetState(raft.Leader)
		}
	}

	// Create addresses for the client
	addresses := []string{
		"localhost:8080",
		"localhost:8081",
		"localhost:8082",
	}

	// Create DistributedClient
	client := raft.NewDistributedClient(addresses)

	// Discover leaders
	err := client.DiscoverLeaders()
	if err != nil {
		t.Fatalf("Failed to discover leaders: %v", err)
	}

	// Test Put operation
	err = client.Put("test-key", "test-value")
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get operation
	value, exists, err := client.Get("test-key")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !exists {
		t.Fatalf("Key should exist but doesn't")
	}

	if value != "test-value" {
		t.Fatalf("Expected 'test-value', got '%s'", value)
	}

	fmt.Println("SUCCESS: End-to-end test passed")
}

func TestPerformance(t *testing.T) {
	// Set up 3 ShardedNodes on different ports
	nodeIDs := []string{"node1", "node2", "node3"}
	ports := []string{"8083", "8084", "8085"}

	// Create ShardedNodes
	nodes := make([]*raft.ShardedNode, 3)
	for i := 0; i < 3; i++ {
		nodes[i] = raft.NewShardedNode(nodeIDs[i], nodeIDs)
		nodes[i].StartHTTPServer(ports[i])
	}

	// Give time for servers to start
	time.Sleep(1 * time.Second)

	// Manually set states for testing - make each node leader for one shard
	for i := 0; i < 3; i++ {
		shard := nodes[i].GetShard(i)
		if shard != nil {
			shard.SetState(raft.Leader)
		}
	}

	// Create addresses for the client
	addresses := []string{
		"localhost:8083",
		"localhost:8084",
		"localhost:8085",
	}

	// Create DistributedClient
	client := raft.NewDistributedClient(addresses)

	// Discover leaders
	err := client.DiscoverLeaders()
	if err != nil {
		t.Fatalf("Failed to discover leaders: %v", err)
	}

	// Run 25000 concurrent Put operations
	numOperations := 25000
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < numOperations; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			key := fmt.Sprintf("key-%d", index)
			value := fmt.Sprintf("value-%d", index)
			err := client.Put(key, value)
			if err != nil {
				t.Errorf("Put failed for key %s: %v", key, err)
			}
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()
	endTime := time.Now()

	// Calculate and print performance metrics
	duration := endTime.Sub(startTime)
	qps := float64(numOperations) / duration.Seconds()

	fmt.Printf("Performance Test Results:\n")
	fmt.Printf("Total operations: %d\n", numOperations)
	fmt.Printf("Total time: %v\n", duration)
	fmt.Printf("Achieved %.2f QPS\n", qps)
}
