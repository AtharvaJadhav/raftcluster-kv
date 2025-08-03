package raft

import (
	"fmt"
	"testing"
)

func TestGetShardForKey(t *testing.T) {
	// Test that the same key always maps to the same shard
	key1 := "user:123"
	shard1 := GetShardForKey(key1)
	shard2 := GetShardForKey(key1)

	if shard1 != shard2 {
		t.Errorf("Same key should map to same shard, got %d and %d", shard1, shard2)
	}

	// Test that shard ID is within valid range
	if shard1 < 0 || shard1 >= NumShards {
		t.Errorf("Shard ID %d is out of valid range [0, %d)", shard1, NumShards)
	}

	// Test with different keys to see distribution
	keys := []string{"user:123", "user:456", "user:789", "config:1", "config:2"}
	shardCounts := make(map[int]int)

	for _, key := range keys {
		shardID := GetShardForKey(key)
		shardCounts[shardID]++

		// Verify shard ID is valid
		if shardID < 0 || shardID >= NumShards {
			t.Errorf("Shard ID %d for key %s is out of valid range [0, %d)", shardID, key, NumShards)
		}
	}

	// Test with empty string
	emptyShard := GetShardForKey("")
	if emptyShard < 0 || emptyShard >= NumShards {
		t.Errorf("Empty string shard ID %d is out of valid range [0, %d)", emptyShard, NumShards)
	}

	// Test with special characters
	specialShard := GetShardForKey("key@#$%^&*()")
	if specialShard < 0 || specialShard >= NumShards {
		t.Errorf("Special characters shard ID %d is out of valid range [0, %d)", specialShard, NumShards)
	}

	t.Logf("Shard distribution: %v", shardCounts)
}

func TestNewShardedNode(t *testing.T) {
	nodeIDs := []string{"node1", "node2", "node3"}
	shardedNode := NewShardedNode("node1", nodeIDs)

	// Test basic ShardedNode properties
	if shardedNode.GetNodeID() != "node1" {
		t.Errorf("Expected nodeID to be 'node1', got %s", shardedNode.GetNodeID())
	}

	// Test that all shards are created
	for shardID := 0; shardID < NumShards; shardID++ {
		raftNode := shardedNode.GetShard(shardID)
		if raftNode == nil {
			t.Errorf("Expected shard %d to be created, got nil", shardID)
			continue
		}

		// Test shard-specific node ID
		expectedNodeID := "node1-shard" + fmt.Sprintf("%d", shardID)
		if raftNode.nodeID != expectedNodeID {
			t.Errorf("Expected shard %d node ID to be %s, got %s", shardID, expectedNodeID, raftNode.nodeID)
		}

		// Test peer list excludes current node
		for _, peerID := range raftNode.peers {
			if peerID == expectedNodeID {
				t.Errorf("Shard %d peer list should not contain current node %s", shardID, expectedNodeID)
			}
		}

		// Test peer list contains other nodes with correct shard-specific IDs
		expectedPeers := []string{"node2-shard" + fmt.Sprintf("%d", shardID), "node3-shard" + fmt.Sprintf("%d", shardID)}
		if len(raftNode.peers) != len(expectedPeers) {
			t.Errorf("Expected %d peers for shard %d, got %d", len(expectedPeers), shardID, len(raftNode.peers))
		}

		// Check that all expected peers are present
		for _, expectedPeer := range expectedPeers {
			found := false
			for _, peer := range raftNode.peers {
				if peer == expectedPeer {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("Expected peer %s not found in shard %d peers", expectedPeer, shardID)
			}
		}
	}
}

func TestShardedNodeClientOperations(t *testing.T) {
	nodeIDs := []string{"node1", "node2", "node3"}
	shardedNode := NewShardedNode("node1", nodeIDs)

	// Make node1 the leader for all shards (for testing purposes)
	for shardID := 0; shardID < NumShards; shardID++ {
		raftNode := shardedNode.GetShard(shardID)
		raftNode.UpdateTerm(1)
		raftNode.SetState(Leader)
	}

	// Test keys that map to different shards
	testCases := []struct {
		key   string
		value string
	}{
		{"user:123", "John Doe"},
		{"user:456", "Jane Smith"},
		{"config:1", "setting1"},
		{"config:2", "setting2"},
		{"data:xyz", "some data"},
	}

	// Test ClientPut operations
	for _, tc := range testCases {
		err := shardedNode.ClientPut(tc.key, tc.value)
		if err != nil {
			t.Errorf("ClientPut failed for key %s: %v", tc.key, err)
		}

		// Manually commit the log entry for testing
		raftNode := shardedNode.GetShard(GetShardForKey(tc.key))
		raftNode.UpdateCommitIndex(int64(len(raftNode.log)))
	}

	// Test ClientGet operations
	for _, tc := range testCases {
		value, exists, err := shardedNode.ClientGet(tc.key)
		if err != nil {
			t.Errorf("ClientGet failed for key %s: %v", tc.key, err)
		}
		if !exists {
			t.Errorf("Key %s should exist after ClientPut", tc.key)
		}
		if value != tc.value {
			t.Errorf("Expected value %s for key %s, got %s", tc.value, tc.key, value)
		}
	}

	// Test non-existent key
	value, exists, err := shardedNode.ClientGet("nonexistent")
	if err != nil {
		t.Errorf("ClientGet failed for non-existent key: %v", err)
	}
	if exists {
		t.Errorf("Non-existent key should not exist")
	}
	if value != "" {
		t.Errorf("Non-existent key should return empty value, got %s", value)
	}
}

func TestShardedNodeUtilityMethods(t *testing.T) {
	nodeIDs := []string{"node1", "node2", "node3"}
	shardedNode := NewShardedNode("node1", nodeIDs)

	// Test initial states
	states := shardedNode.GetShardStates()
	if len(states) != NumShards {
		t.Errorf("Expected %d shard states, got %d", NumShards, len(states))
	}

	// All shards should start as Followers
	for shardID, state := range states {
		if state != Follower {
			t.Errorf("Expected shard %d to start as Follower, got %v", shardID, state)
		}
	}

	// Test initial leadership (should be none)
	leaders := shardedNode.GetShardLeaders()
	if len(leaders) != 0 {
		t.Errorf("Expected no leaders initially, got %d", len(leaders))
	}

	// Test IsShardLeader for all shards (should all be false initially)
	for shardID := 0; shardID < NumShards; shardID++ {
		if shardedNode.IsShardLeader(shardID) {
			t.Errorf("Expected shard %d to not be leader initially", shardID)
		}
	}

	// Test invalid shard ID
	if shardedNode.IsShardLeader(-1) {
		t.Errorf("Expected invalid shard ID -1 to return false")
	}
	if shardedNode.IsShardLeader(NumShards) {
		t.Errorf("Expected invalid shard ID %d to return false", NumShards)
	}

	// Make node1 leader for shard 0
	shard0Node := shardedNode.GetShard(0)
	shard0Node.UpdateTerm(1)
	shard0Node.SetState(Leader)

	// Test leadership detection
	if !shardedNode.IsShardLeader(0) {
		t.Errorf("Expected shard 0 to be leader after setting state")
	}

	leaders = shardedNode.GetShardLeaders()
	if len(leaders) != 1 {
		t.Errorf("Expected 1 leader, got %d", len(leaders))
	}
	if nodeID, exists := leaders[0]; !exists || nodeID != "node1" {
		t.Errorf("Expected leader to be 'node1', got %s", nodeID)
	}

	// Test updated states
	states = shardedNode.GetShardStates()
	if states[0] != Leader {
		t.Errorf("Expected shard 0 state to be Leader, got %v", states[0])
	}

	// Make node1 leader for shard 2 as well
	shard2Node := shardedNode.GetShard(2)
	shard2Node.UpdateTerm(1)
	shard2Node.SetState(Leader)

	// Test multiple leadership
	leaders = shardedNode.GetShardLeaders()
	if len(leaders) != 2 {
		t.Errorf("Expected 2 leaders, got %d", len(leaders))
	}

	// Verify both shards 0 and 2 are led by node1
	if !shardedNode.IsShardLeader(0) || !shardedNode.IsShardLeader(2) {
		t.Errorf("Expected shards 0 and 2 to be led by node1")
	}

	// Shard 1 should still be follower
	if shardedNode.IsShardLeader(1) {
		t.Errorf("Expected shard 1 to not be leader")
	}
}

func TestShardedNodeConcurrentAccess(t *testing.T) {
	nodeIDs := []string{"node1", "node2", "node3"}
	shardedNode := NewShardedNode("node1", nodeIDs)

	// Test concurrent access to utility methods
	done := make(chan bool, 10)

	for i := 0; i < 5; i++ {
		go func() {
			shardedNode.GetShardStates()
			shardedNode.GetShardLeaders()
			shardedNode.GetNodeID()
			done <- true
		}()
	}

	for i := 0; i < 5; i++ {
		go func() {
			shardedNode.IsShardLeader(0)
			shardedNode.IsShardLeader(1)
			shardedNode.IsShardLeader(2)
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify the node is still in a consistent state
	nodeID := shardedNode.GetNodeID()
	if nodeID != "node1" {
		t.Errorf("Expected nodeID to remain 'node1' after concurrent access, got %s", nodeID)
	}
}
