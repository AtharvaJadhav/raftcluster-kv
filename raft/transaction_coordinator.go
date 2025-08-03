package raft

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// TransactionStatus represents the current state of a transaction
type TransactionStatus int

const (
	PREPARING TransactionStatus = iota
	COMMITTED
	ABORTED
)

// String returns the string representation of TransactionStatus
func (ts TransactionStatus) String() string {
	switch ts {
	case PREPARING:
		return "PREPARING"
	case COMMITTED:
		return "COMMITTED"
	case ABORTED:
		return "ABORTED"
	default:
		return "UNKNOWN"
	}
}

// TransactionOperation represents a single operation within a transaction
type TransactionOperation struct {
	Type    string // "GET" or "PUT"
	Key     string
	Value   string // Empty for GET operations
	ShardID int
	Phase   string // "PREPARE", "COMMIT", "ABORT" for two-phase commit
}

// Transaction represents a multi-shard transaction
type Transaction struct {
	ID             string                 // Unique transaction identifier
	SequenceNumber int64                  // Global ordering number
	Shards         []int                  // Which shards are involved
	Operations     []TransactionOperation // List of operations in this transaction
	Status         TransactionStatus      // Current status of the transaction
	CreatedAt      time.Time              // When the transaction was created
}

// MultiShardKeyResult represents the result of reading a single key from a shard
type MultiShardKeyResult struct {
	Key     string // The key that was read
	Value   string // The value (empty if key doesn't exist)
	Exists  bool   // Whether the key exists
	ShardID int    // Which shard this key belongs to
	Error   string // Error message (empty if no error)
}

// MultiShardReadResult represents the result of a multi-shard read operation
type MultiShardReadResult struct {
	TransactionID  string                         // Unique transaction identifier
	SequenceNumber int64                          // Global ordering number
	Results        map[string]MultiShardKeyResult // Key -> result mapping
	ReadTimestamp  time.Time                      // When the read operation was performed
}

// MultiShardWriteResult represents the result of a multi-shard write operation
type MultiShardWriteResult struct {
	TransactionID  string    // Unique transaction identifier
	SequenceNumber int64     // Global ordering number
	Success        bool      // Whether the write operation succeeded
	FailedShards   []int     // Which shards failed, if any
	WriteTimestamp time.Time // When the write operation was performed
	Error          string    // Error message (empty if successful)
}

// TransactionCoordinator manages cross-shard operations with strong consistency guarantees
type TransactionCoordinator struct {
	nodeID               string                  // Which node is running this coordinator
	shardedNode          *ShardedNode            // Reference to the local sharded node
	globalSequenceNumber int64                   // Atomic counter for ordering operations
	activeTransactions   map[string]*Transaction // Tracks ongoing transactions
	mu                   sync.RWMutex            // For thread safety
}

// NewTransactionCoordinator creates a new TransactionCoordinator
func NewTransactionCoordinator(nodeID string, shardedNode *ShardedNode) *TransactionCoordinator {
	return &TransactionCoordinator{
		nodeID:               nodeID,
		shardedNode:          shardedNode,
		globalSequenceNumber: 0,
		activeTransactions:   make(map[string]*Transaction),
		mu:                   sync.RWMutex{},
	}
}

// generateTransactionID creates a unique transaction identifier
// Format: "txn-{nodeID}-{timestamp}"
func (tc *TransactionCoordinator) generateTransactionID() string {
	timestamp := time.Now().UnixNano()
	return fmt.Sprintf("txn-%s-%d", tc.nodeID, timestamp)
}

// getNextSequenceNumber atomically increments and returns the next global sequence number
func (tc *TransactionCoordinator) getNextSequenceNumber() int64 {
	return atomic.AddInt64(&tc.globalSequenceNumber, 1)
}

// GetNodeID returns the node ID of this coordinator
func (tc *TransactionCoordinator) GetNodeID() string {
	return tc.nodeID
}

// GetShardedNode returns the reference to the sharded node
func (tc *TransactionCoordinator) GetShardedNode() *ShardedNode {
	return tc.shardedNode
}

// GetActiveTransactionCount returns the number of currently active transactions
func (tc *TransactionCoordinator) GetActiveTransactionCount() int {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	return len(tc.activeTransactions)
}

// GetTransaction retrieves a transaction by its ID
func (tc *TransactionCoordinator) GetTransaction(transactionID string) (*Transaction, bool) {
	tc.mu.RLock()
	defer tc.mu.RUnlock()
	transaction, exists := tc.activeTransactions[transactionID]
	return transaction, exists
}

// AddTransaction adds a new transaction to the active transactions map
func (tc *TransactionCoordinator) AddTransaction(transaction *Transaction) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	tc.activeTransactions[transaction.ID] = transaction
}

// RemoveTransaction removes a transaction from the active transactions map
func (tc *TransactionCoordinator) RemoveTransaction(transactionID string) {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	delete(tc.activeTransactions, transactionID)
}

// UpdateTransactionStatus updates the status of a transaction
func (tc *TransactionCoordinator) UpdateTransactionStatus(transactionID string, status TransactionStatus) bool {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	transaction, exists := tc.activeTransactions[transactionID]
	if !exists {
		return false
	}

	transaction.Status = status
	return true
}

// MultiShardPut performs a multi-shard write operation using two-phase commit
func (tc *TransactionCoordinator) MultiShardPut(operations map[string]string) (*MultiShardWriteResult, error) {
	if len(operations) == 0 {
		return nil, fmt.Errorf("no operations provided for multi-shard write")
	}

	// Create a new transaction for this write operation
	transactionID := tc.generateTransactionID()
	sequenceNumber := tc.getNextSequenceNumber()

	// Convert operations map to TransactionOperation slice
	var transactionOperations []TransactionOperation
	var involvedShards []int
	shardSet := make(map[int]bool)

	for key, value := range operations {
		shardID := GetShardForKey(key)
		shardSet[shardID] = true

		transactionOperations = append(transactionOperations, TransactionOperation{
			Type:    "PUT",
			Key:     key,
			Value:   value,
			ShardID: shardID,
			Phase:   "PREPARE", // Initial phase
		})
	}

	// Convert shard set to slice
	for shardID := range shardSet {
		involvedShards = append(involvedShards, shardID)
	}

	// Create and register the transaction
	transaction := &Transaction{
		ID:             transactionID,
		SequenceNumber: sequenceNumber,
		Shards:         involvedShards,
		Operations:     transactionOperations,
		Status:         PREPARING,
		CreatedAt:      time.Now(),
	}

	tc.AddTransaction(transaction)

	// Initialize result structure
	result := &MultiShardWriteResult{
		TransactionID:  transactionID,
		SequenceNumber: sequenceNumber,
		Success:        false,
		FailedShards:   []int{},
		WriteTimestamp: time.Now(),
		Error:          "",
	}

	// Group operations by shard for easier processing
	shardGroups := tc.groupOperationsByShard(transactionOperations)

	// PHASE 1: PREPARE
	// Send prepare requests to all involved shards
	prepareResults := make(map[int]bool)
	var prepareWg sync.WaitGroup
	var prepareMu sync.Mutex

	for shardID, shardOperations := range shardGroups {
		prepareWg.Add(1)
		go func(shardID int, operations []TransactionOperation) {
			defer prepareWg.Done()

			success := tc.prepareShard(shardID, operations, transactionID)

			prepareMu.Lock()
			prepareResults[shardID] = success
			prepareMu.Unlock()
		}(shardID, shardOperations)
	}

	prepareWg.Wait()

	// Check if all shards are prepared
	allPrepared := true
	var failedShards []int
	for shardID, success := range prepareResults {
		if !success {
			allPrepared = false
			failedShards = append(failedShards, shardID)
		}
	}

	if !allPrepared {
		// PHASE 2A: ABORT
		// Some shards failed to prepare, abort all shards
		tc.UpdateTransactionStatus(transactionID, ABORTED)

		var abortWg sync.WaitGroup
		for shardID := range shardGroups {
			abortWg.Add(1)
			go func(shardID int) {
				defer abortWg.Done()
				tc.abortShard(shardID, transactionID)
			}(shardID)
		}
		abortWg.Wait()

		result.Success = false
		result.FailedShards = failedShards
		result.Error = fmt.Sprintf("prepare phase failed for shards: %v", failedShards)

		// Clean up the transaction
		go func() {
			time.Sleep(5 * time.Second)
			tc.RemoveTransaction(transactionID)
		}()

		return result, nil
	}

	// PHASE 2B: COMMIT
	// All shards are prepared, proceed with commit
	tc.UpdateTransactionStatus(transactionID, PREPARING) // Still preparing until commit completes

	var commitWg sync.WaitGroup
	var commitMu sync.Mutex
	var commitFailedShards []int

	for shardID := range shardGroups {
		commitWg.Add(1)
		go func(shardID int) {
			defer commitWg.Done()

			success := tc.commitShard(shardID, transactionID)

			commitMu.Lock()
			if !success {
				commitFailedShards = append(commitFailedShards, shardID)
			}
			commitMu.Unlock()
		}(shardID)
	}

	commitWg.Wait()

	if len(commitFailedShards) > 0 {
		// Some shards failed to commit - this is a critical failure
		// In a real system, we would need to implement recovery mechanisms
		tc.UpdateTransactionStatus(transactionID, ABORTED)

		result.Success = false
		result.FailedShards = commitFailedShards
		result.Error = fmt.Sprintf("commit phase failed for shards: %v", commitFailedShards)
	} else {
		// All shards committed successfully
		tc.UpdateTransactionStatus(transactionID, COMMITTED)

		result.Success = true
		result.FailedShards = []int{}
		result.Error = ""
	}

	// Clean up the transaction after a short delay
	go func() {
		time.Sleep(5 * time.Second)
		tc.RemoveTransaction(transactionID)
	}()

	return result, nil
}

// GetAllActiveTransactions returns a copy of all active transactions
func (tc *TransactionCoordinator) GetAllActiveTransactions() map[string]*Transaction {
	tc.mu.RLock()
	defer tc.mu.RUnlock()

	// Create a copy to avoid external modification
	transactions := make(map[string]*Transaction)
	for id, transaction := range tc.activeTransactions {
		transactions[id] = transaction
	}
	return transactions
}

// groupKeysByShard groups keys by their target shard IDs
func (tc *TransactionCoordinator) groupKeysByShard(keys []string) map[int][]string {
	shardGroups := make(map[int][]string)

	for _, key := range keys {
		shardID := GetShardForKey(key)
		shardGroups[shardID] = append(shardGroups[shardID], key)
	}

	return shardGroups
}

// groupOperationsByShard groups operations by their target shard IDs
func (tc *TransactionCoordinator) groupOperationsByShard(operations []TransactionOperation) map[int][]TransactionOperation {
	shardGroups := make(map[int][]TransactionOperation)

	for _, operation := range operations {
		shardGroups[operation.ShardID] = append(shardGroups[operation.ShardID], operation)
	}

	return shardGroups
}

// MultiShardGet performs a multi-shard read operation with global ordering
func (tc *TransactionCoordinator) MultiShardGet(keys []string) (*MultiShardReadResult, error) {
	if len(keys) == 0 {
		return nil, fmt.Errorf("no keys provided for multi-shard read")
	}

	// Create a new transaction for this read operation
	transactionID := tc.generateTransactionID()
	sequenceNumber := tc.getNextSequenceNumber()

	// Group keys by their target shards
	shardGroups := tc.groupKeysByShard(keys)

	// Create transaction operations for each key
	var operations []TransactionOperation
	var involvedShards []int

	for shardID, shardKeys := range shardGroups {
		involvedShards = append(involvedShards, shardID)
		for _, key := range shardKeys {
			operations = append(operations, TransactionOperation{
				Type:    "GET",
				Key:     key,
				Value:   "", // Empty for GET operations
				ShardID: shardID,
			})
		}
	}

	// Create and register the transaction
	transaction := &Transaction{
		ID:             transactionID,
		SequenceNumber: sequenceNumber,
		Shards:         involvedShards,
		Operations:     operations,
		Status:         PREPARING,
		CreatedAt:      time.Now(),
	}

	tc.AddTransaction(transaction)

	// Initialize result structure
	result := &MultiShardReadResult{
		TransactionID:  transactionID,
		SequenceNumber: sequenceNumber,
		Results:        make(map[string]MultiShardKeyResult),
		ReadTimestamp:  time.Now(),
	}

	// Use a WaitGroup to coordinate concurrent reads
	var wg sync.WaitGroup
	var mu sync.Mutex // Protect concurrent writes to result.Results

	// Read from each shard concurrently
	for shardID, shardKeys := range shardGroups {
		wg.Add(1)
		go func(shardID int, keys []string) {
			defer wg.Done()

			// Get the RaftNode for this shard
			raftNode := tc.shardedNode.GetShard(shardID)
			if raftNode == nil {
				// Shard is unavailable
				mu.Lock()
				for _, key := range keys {
					result.Results[key] = MultiShardKeyResult{
						Key:     key,
						Value:   "",
						Exists:  false,
						ShardID: shardID,
						Error:   fmt.Sprintf("shard %d is unavailable", shardID),
					}
				}
				mu.Unlock()
				return
			}

			// Check if this node is the leader for this shard
			if !tc.shardedNode.IsShardLeader(shardID) {
				// Not the leader, return error
				mu.Lock()
				for _, key := range keys {
					result.Results[key] = MultiShardKeyResult{
						Key:     key,
						Value:   "",
						Exists:  false,
						ShardID: shardID,
						Error:   fmt.Sprintf("not leader for shard %d", shardID),
					}
				}
				mu.Unlock()
				return
			}

			// Read each key from this shard
			for _, key := range keys {
				value, exists, err := raftNode.ClientGet(key)

				mu.Lock()
				if err != nil {
					result.Results[key] = MultiShardKeyResult{
						Key:     key,
						Value:   "",
						Exists:  false,
						ShardID: shardID,
						Error:   err.Error(),
					}
				} else {
					result.Results[key] = MultiShardKeyResult{
						Key:     key,
						Value:   value,
						Exists:  exists,
						ShardID: shardID,
						Error:   "",
					}
				}
				mu.Unlock()
			}
		}(shardID, shardKeys)
	}

	// Wait for all concurrent reads to complete
	wg.Wait()

	// Mark transaction as committed since reads are immediately visible
	tc.UpdateTransactionStatus(transactionID, COMMITTED)

	// Clean up the transaction after a short delay (for monitoring purposes)
	go func() {
		time.Sleep(5 * time.Second)
		tc.RemoveTransaction(transactionID)
	}()

	return result, nil
}

// prepareShard sends a prepare request to a specific shard
// Returns true if the shard is ready to commit, false otherwise
func (tc *TransactionCoordinator) prepareShard(shardID int, operations []TransactionOperation, transactionID string) bool {
	// Get the RaftNode for this shard
	raftNode := tc.shardedNode.GetShard(shardID)
	if raftNode == nil {
		return false // Shard is unavailable
	}

	// Check if this node is the leader for this shard
	if !tc.shardedNode.IsShardLeader(shardID) {
		return false // Not the leader
	}

	// For now, we'll simulate the prepare phase by checking if the shard is available
	// In a real implementation, this would involve:
	// 1. Locking the keys in the shard
	// 2. Validating the operations
	// 3. Preparing the transaction state

	// Simulate some preparation time
	time.Sleep(10 * time.Millisecond)

	// For this implementation, we'll assume the shard is always ready to commit
	// In practice, this would check if the shard can accommodate the operations
	return true
}

// commitShard sends a commit request to a specific shard
// Returns true if the commit was successful, false otherwise
func (tc *TransactionCoordinator) commitShard(shardID int, transactionID string) bool {
	// Get the RaftNode for this shard
	raftNode := tc.shardedNode.GetShard(shardID)
	if raftNode == nil {
		return false // Shard is unavailable
	}

	// Check if this node is the leader for this shard
	if !tc.shardedNode.IsShardLeader(shardID) {
		return false // Not the leader
	}

	// Get the transaction to find the operations for this shard
	transaction, exists := tc.GetTransaction(transactionID)
	if !exists {
		return false // Transaction not found
	}

	// Find operations for this shard
	var shardOperations []TransactionOperation
	for _, operation := range transaction.Operations {
		if operation.ShardID == shardID && operation.Type == "PUT" {
			shardOperations = append(shardOperations, operation)
		}
	}

	// Execute the PUT operations on this shard
	for _, operation := range shardOperations {
		err := raftNode.ClientPut(operation.Key, operation.Value)
		if err != nil {
			return false // Commit failed
		}
	}

	return true
}

// abortShard sends an abort request to a specific shard
// Returns true if the abort was successful, false otherwise
func (tc *TransactionCoordinator) abortShard(shardID int, transactionID string) bool {
	// Get the RaftNode for this shard
	raftNode := tc.shardedNode.GetShard(shardID)
	if raftNode == nil {
		return false // Shard is unavailable
	}

	// Check if this node is the leader for this shard
	if !tc.shardedNode.IsShardLeader(shardID) {
		return false // Not the leader
	}

	// In a real implementation, this would:
	// 1. Release any locks held by the transaction
	// 2. Rollback any prepared state
	// 3. Clean up transaction metadata

	// For this implementation, we'll just return true since we haven't actually
	// prepared any state that needs to be rolled back
	return true
}
