package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

// DistributedClient represents a client that can interact with the distributed sharded system
type DistributedClient struct {
	nodeAddresses      []string       // List of node addresses in the cluster
	leaderCache        map[int]string // Maps shard ID to leader node address
	httpClient         *http.Client   // HTTP client for making requests
	concurrentRequests int            // Maximum number of concurrent requests
	semaphore          chan struct{}  // Semaphore to limit concurrent requests
	mu                 sync.RWMutex   // Mutex for thread safety
}

// NewDistributedClient creates a new DistributedClient with the given node addresses
func NewDistributedClient(nodeAddresses []string) *DistributedClient {
	// Create custom transport with connection pooling
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 100,
		IdleConnTimeout:     90 * time.Second,
	}

	// Create HTTP client with custom transport and timeout
	httpClient := &http.Client{
		Transport: transport,
		Timeout:   1 * time.Second,
	}

	return &DistributedClient{
		nodeAddresses:      nodeAddresses,
		leaderCache:        make(map[int]string),
		httpClient:         httpClient,
		concurrentRequests: 2000,
		semaphore:          make(chan struct{}, 2000),
	}
}

// DiscoverLeaders discovers which nodes are leading which shards via HTTP
func (dc *DistributedClient) DiscoverLeaders() error {
	dc.mu.Lock()
	defer dc.mu.Unlock()

	// Clear existing cache
	dc.leaderCache = make(map[int]string)

	// Create a mapping from node IDs to addresses
	nodeIDToAddress := make(map[string]string)
	for i, nodeAddress := range dc.nodeAddresses {
		// Extract node ID from address (assuming format like "localhost:8080")
		nodeID := fmt.Sprintf("node%d", i+1)
		nodeIDToAddress[nodeID] = nodeAddress
	}

	// Loop through all node addresses
	for _, nodeAddress := range dc.nodeAddresses {
		// Make HTTP GET request to each node's /leaders endpoint
		url := "http://" + nodeAddress + "/leaders"
		resp, err := dc.httpClient.Get(url)
		if err != nil {
			log.Printf("Failed to connect to %s: %v", nodeAddress, err)
			continue // Skip this node and try the next one
		}
		defer resp.Body.Close()

		// Parse JSON response into map[int]string
		var nodeLeaders map[int]string
		if err := json.NewDecoder(resp.Body).Decode(&nodeLeaders); err != nil {
			log.Printf("Failed to decode response from %s: %v", nodeAddress, err)
			continue
		}

		// Merge results to build complete leader picture for all shards
		for shardID, leaderNodeID := range nodeLeaders {
			// Convert leader node ID to address
			if leaderAddress, exists := nodeIDToAddress[leaderNodeID]; exists {
				dc.leaderCache[shardID] = leaderAddress
			}
		}
	}

	return nil
}

// GetLeaderForShard returns the cached leader address for the specified shard
// Returns the leader address and whether it was found in the cache
func (dc *DistributedClient) GetLeaderForShard(shardID int) (string, bool) {
	dc.mu.RLock()
	defer dc.mu.RUnlock()

	leader, found := dc.leaderCache[shardID]
	return leader, found
}

// Get retrieves a value by key, automatically routing to the correct shard
// Returns (value, exists, error)
func (dc *DistributedClient) Get(key string) (string, bool, error) {
	// Acquire semaphore to limit concurrent requests
	dc.semaphore <- struct{}{}
	defer func() { <-dc.semaphore }()

	// Determine which shard handles this key
	shardID := GetShardForKey(key)

	// Retry loop for failover handling (max 2 attempts)
	for attempt := 0; attempt < 2; attempt++ {
		// Find the leader for this shard
		leaderAddress, found := dc.GetLeaderForShard(shardID)
		if !found {
			// If no leader found and this is the first attempt, try to discover leaders
			if attempt == 0 {
				if err := dc.DiscoverLeaders(); err != nil {
					return "", false, fmt.Errorf("failed to discover leaders: %v", err)
				}
				continue // Try again with refreshed cache
			}
			return "", false, fmt.Errorf("no leader found for shard %d", shardID)
		}

		// Make HTTP GET request to the leader
		url := fmt.Sprintf("http://%s/get?key=%s", leaderAddress, key)
		resp, err := dc.httpClient.Get(url)
		if err != nil {
			// If this is the first attempt, refresh leaders and retry
			if attempt == 0 {
				if refreshErr := dc.DiscoverLeaders(); refreshErr != nil {
					return "", false, fmt.Errorf("HTTP request failed and leader refresh failed: %v, refresh error: %v", err, refreshErr)
				}
				continue // Try again with refreshed cache
			}
			return "", false, fmt.Errorf("HTTP request failed after retry: %v", err)
		}
		defer resp.Body.Close()

		// Check if response status is OK
		if resp.StatusCode != http.StatusOK {
			// If this is the first attempt, refresh leaders and retry
			if attempt == 0 {
				if refreshErr := dc.DiscoverLeaders(); refreshErr != nil {
					return "", false, fmt.Errorf("HTTP request failed with status %s and leader refresh failed: %v", resp.Status, refreshErr)
				}
				continue // Try again with refreshed cache
			}
			return "", false, fmt.Errorf("HTTP request failed with status: %s", resp.Status)
		}

		// Parse the JSON response
		var response struct {
			Value  string `json:"value"`
			Exists bool   `json:"exists"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
			// If this is the first attempt, refresh leaders and retry
			if attempt == 0 {
				if refreshErr := dc.DiscoverLeaders(); refreshErr != nil {
					return "", false, fmt.Errorf("failed to decode JSON response and leader refresh failed: %v, refresh error: %v", err, refreshErr)
				}
				continue // Try again with refreshed cache
			}
			return "", false, fmt.Errorf("failed to decode JSON response: %v", err)
		}

		// Success - return the result
		return response.Value, response.Exists, nil
	}

	return "", false, fmt.Errorf("all retry attempts failed for shard %d", shardID)
}

// Put stores a key-value pair, automatically routing to the correct shard
// Returns error if the operation fails
func (dc *DistributedClient) Put(key, value string) error {
	// Acquire semaphore to limit concurrent requests
	dc.semaphore <- struct{}{}
	defer func() { <-dc.semaphore }()

	// Determine which shard handles this key
	shardID := GetShardForKey(key)

	// Retry loop for failover handling (max 2 attempts)
	for attempt := 0; attempt < 2; attempt++ {
		// Find the leader for this shard
		leaderAddress, found := dc.GetLeaderForShard(shardID)
		if !found {
			// If no leader found and this is the first attempt, try to discover leaders
			if attempt == 0 {
				if err := dc.DiscoverLeaders(); err != nil {
					return fmt.Errorf("failed to discover leaders: %v", err)
				}
				continue // Try again with refreshed cache
			}
			return fmt.Errorf("no leader found for shard %d", shardID)
		}

		// Make HTTP GET request to the leader (using GET for simplicity with query params)
		url := fmt.Sprintf("http://%s/put?key=%s&value=%s", leaderAddress, key, value)
		resp, err := dc.httpClient.Get(url)
		if err != nil {
			// If this is the first attempt, refresh leaders and retry
			if attempt == 0 {
				if refreshErr := dc.DiscoverLeaders(); refreshErr != nil {
					return fmt.Errorf("HTTP request failed and leader refresh failed: %v, refresh error: %v", err, refreshErr)
				}
				continue // Try again with refreshed cache
			}
			return fmt.Errorf("HTTP request failed after retry: %v", err)
		}
		defer resp.Body.Close()

		// Check if response status is OK
		if resp.StatusCode != http.StatusOK {
			// If this is the first attempt, refresh leaders and retry
			if attempt == 0 {
				if refreshErr := dc.DiscoverLeaders(); refreshErr != nil {
					return fmt.Errorf("HTTP request failed with status %s and leader refresh failed: %v", resp.Status, refreshErr)
				}
				continue // Try again with refreshed cache
			}
			return fmt.Errorf("HTTP request failed with status: %s", resp.Status)
		}

		// Success - return nil
		return nil
	}

	return fmt.Errorf("all retry attempts failed for shard %d", shardID)
}
