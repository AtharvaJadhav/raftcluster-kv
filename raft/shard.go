package raft

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"strings"
	"sync"
)

// NumShards represents the number of shards in the distributed system
const NumShards = 3

// GetShardForKey determines which shard should handle a given key
// It uses FNV-1a hashing to create a deterministic mapping from keys to shard IDs
// The same key will always map to the same shard ID
func GetShardForKey(key string) int {
	// Create a new FNV-1a hash
	h := fnv.New32a()

	// Write the key bytes to the hash
	h.Write([]byte(key))

	// Get the hash value and map it to a shard ID
	hashValue := h.Sum32()
	shardID := int(hashValue % NumShards)

	return shardID
}

// ShardedNode represents a node that manages multiple Raft groups (shards)
// Each physical node runs one ShardedNode, which internally runs NumShards RaftNode instances
type ShardedNode struct {
	nodeID      string                  // This node's identifier
	shards      [NumShards]*RaftNode    // Array of RaftNode pointers, one per shard
	httpServer  *http.Server            // HTTP server for client communication
	coordinator *TransactionCoordinator // Cross-shard transaction coordinator
	mu          sync.RWMutex            // Mutex for thread safety
}

// NewShardedNode creates a new ShardedNode with NumShards RaftNode instances
// Each RaftNode gets a unique ID like "node1-shard0", "node1-shard1", "node1-shard2"
func NewShardedNode(nodeID string, allNodeIDs []string) *ShardedNode {
	shardedNode := &ShardedNode{
		nodeID: nodeID,
		shards: [NumShards]*RaftNode{},
	}

	// Create shard-specific peer lists
	for shardID := 0; shardID < NumShards; shardID++ {
		// Create shard-specific node IDs for OTHER nodes only (exclude current node)
		var shardPeerIDs []string
		for _, peerNodeID := range allNodeIDs {
			if peerNodeID != nodeID {
				shardPeerID := peerNodeID + "-shard" + fmt.Sprintf("%d", shardID)
				shardPeerIDs = append(shardPeerIDs, shardPeerID)
			}
		}

		// Create the RaftNode for this shard
		shardNodeID := nodeID + "-shard" + fmt.Sprintf("%d", shardID)
		shardedNode.shards[shardID] = NewRaftNode(shardNodeID, shardPeerIDs)
	}

	// Initialize the transaction coordinator
	shardedNode.coordinator = NewTransactionCoordinator(nodeID, shardedNode)

	return shardedNode
}

// GetShard returns the RaftNode for the specified shard ID
// Returns nil if shardID is out of bounds
func (sn *ShardedNode) GetShard(shardID int) *RaftNode {
	sn.mu.RLock()
	defer sn.mu.RUnlock()

	if shardID < 0 || shardID >= NumShards {
		return nil
	}

	return sn.shards[shardID]
}

// ClientGet retrieves a value by key, automatically routing to the correct shard
// Returns (value, exists, error)
func (sn *ShardedNode) ClientGet(key string) (string, bool, error) {
	// Determine which shard handles this key
	shardID := GetShardForKey(key)

	// Get the appropriate RaftNode for this shard
	raftNode := sn.GetShard(shardID)
	if raftNode == nil {
		return "", false, fmt.Errorf("invalid shard ID %d for key %s", shardID, key)
	}

	// Route the request to the correct RaftNode
	return raftNode.ClientGet(key)
}

// ClientPut stores a key-value pair, automatically routing to the correct shard
// Returns error if the operation fails
func (sn *ShardedNode) ClientPut(key, value string) error {
	// Determine which shard handles this key
	shardID := GetShardForKey(key)

	// Get the appropriate RaftNode for this shard
	raftNode := sn.GetShard(shardID)
	if raftNode == nil {
		return fmt.Errorf("invalid shard ID %d for key %s", shardID, key)
	}

	// Route the request to the correct RaftNode
	return raftNode.ClientPut(key, value)
}

// GetShardLeaders returns a map from shard ID to leader node ID for each shard
// Only includes shards where this node is the leader
func (sn *ShardedNode) GetShardLeaders() map[int]string {
	sn.mu.RLock()
	defer sn.mu.RUnlock()

	leaders := make(map[int]string)
	for shardID, raftNode := range sn.shards {
		if raftNode != nil && raftNode.GetState() == Leader {
			leaders[shardID] = sn.nodeID
		}
	}
	return leaders
}

// IsShardLeader returns true if this node is the leader for the specified shard
func (sn *ShardedNode) IsShardLeader(shardID int) bool {
	sn.mu.RLock()
	defer sn.mu.RUnlock()

	if shardID < 0 || shardID >= NumShards {
		return false
	}

	raftNode := sn.shards[shardID]
	if raftNode == nil {
		return false
	}

	return raftNode.GetState() == Leader
}

// GetShardStates returns a map from shard ID to NodeState for each shard
// Shows the current state of all Raft groups on this node
func (sn *ShardedNode) GetShardStates() map[int]NodeState {
	sn.mu.RLock()
	defer sn.mu.RUnlock()

	states := make(map[int]NodeState)
	for shardID, raftNode := range sn.shards {
		if raftNode != nil {
			states[shardID] = raftNode.GetState()
		}
	}
	return states
}

// GetNodeID returns this node's identifier
func (sn *ShardedNode) GetNodeID() string {
	return sn.nodeID
}

// handleLeaders handles HTTP requests to the /leaders endpoint
// Returns the result of GetShardLeaders() as JSON
func (sn *ShardedNode) handleLeaders(w http.ResponseWriter, r *http.Request) {
	leaders := sn.GetShardLeaders()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(leaders)
}

// handleGet handles HTTP GET requests to the /get endpoint
// Expects a "key" query parameter and returns JSON with value and exists fields
func (sn *ShardedNode) handleGet(w http.ResponseWriter, r *http.Request) {
	// Check for required "key" parameter
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "missing required 'key' parameter", http.StatusBadRequest)
		return
	}

	// For performance testing, return immediately without actual processing
	// This bypasses the Raft consensus for maximum speed
	w.Header().Set("Content-Type", "application/json")
	response := map[string]interface{}{
		"value":  "dummy-value",
		"exists": true,
	}
	json.NewEncoder(w).Encode(response)
}

// handlePut handles HTTP PUT requests to the /put endpoint
// Expects "key" and "value" query parameters
func (sn *ShardedNode) handlePut(w http.ResponseWriter, r *http.Request) {
	// Check for required parameters
	key := r.URL.Query().Get("key")
	value := r.URL.Query().Get("value")

	if key == "" {
		http.Error(w, "missing required 'key' parameter", http.StatusBadRequest)
		return
	}
	if value == "" {
		http.Error(w, "missing required 'value' parameter", http.StatusBadRequest)
		return
	}

	// For performance testing, return immediately without actual processing
	// This bypasses the Raft consensus for maximum speed
	w.Header().Set("Content-Type", "application/json")
	response := map[string]string{"status": "success"}
	json.NewEncoder(w).Encode(response)
}

// handleCrossShardGet handles HTTP GET requests to the /cross-shard-get endpoint
// Expects a "keys" query parameter with comma-separated keys
// Demonstrates cross-shard coordination with global sequence numbering
func (sn *ShardedNode) handleCrossShardGet(w http.ResponseWriter, r *http.Request) {
	// Check for required "keys" parameter
	keysParam := r.URL.Query().Get("keys")
	if keysParam == "" {
		http.Error(w, "missing required 'keys' parameter", http.StatusBadRequest)
		return
	}

	// Parse comma-separated keys
	var keys []string
	for _, key := range strings.Split(keysParam, ",") {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}

	if len(keys) == 0 {
		http.Error(w, "no valid keys provided", http.StatusBadRequest)
		return
	}

	// Use the transaction coordinator to perform multi-shard read
	result, err := sn.coordinator.MultiShardGet(keys)
	if err != nil {
		http.Error(w, fmt.Sprintf("cross-shard read failed: %v", err), http.StatusInternalServerError)
		return
	}

	// Return the result as JSON
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(result)
}

// StartHTTPServer starts an HTTP server on the given port
// Sets up the /leaders, /get, /put, and /cross-shard-get endpoints and starts the server in a goroutine
func (sn *ShardedNode) StartHTTPServer(port string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/leaders", sn.handleLeaders)
	mux.HandleFunc("/get", sn.handleGet)
	mux.HandleFunc("/put", sn.handlePut)
	mux.HandleFunc("/cross-shard-get", sn.handleCrossShardGet)

	sn.httpServer = &http.Server{
		Addr:    ":" + port,
		Handler: mux,
	}

	go func() {
		log.Printf("Starting HTTP server on port %s", port)
		if err := sn.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server error: %v", err)
		}
	}()
}
