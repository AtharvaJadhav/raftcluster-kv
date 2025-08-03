package raft

import (
	"errors"
	"sync"
)

// PutCommand represents a put operation in the log
type PutCommand struct {
	Key   string
	Value string
}

// KeyValueStore represents a simple key-value storage
type KeyValueStore struct {
	data map[string]string
	mu   sync.RWMutex
}

// NewKeyValueStore creates a new empty key-value store
func NewKeyValueStore() *KeyValueStore {
	return &KeyValueStore{
		data: make(map[string]string),
	}
}

// Get retrieves a value by key
func (kv *KeyValueStore) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	value, exists := kv.data[key]
	return value, exists
}

// Put stores a key-value pair
func (kv *KeyValueStore) Put(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[key] = value
}

// LogEntry represents a single entry in the Raft log
type LogEntry struct {
	Term    int64       // Leadership term when entry was created
	Index   int64       // Position in log, starting from 1
	Command interface{} // The actual operation to execute
}

// NodeState represents the current state of a Raft node
type NodeState int

const (
	Follower NodeState = iota
	Candidate
	Leader
)

// RaftNode represents a single node in the Raft cluster
type RaftNode struct {
	mu          sync.RWMutex   // Mutex for thread safety
	currentTerm int64          // Latest term this node has seen
	votedFor    string         // Candidate that received vote in current term
	log         []LogEntry     // Sequence of log entries
	commitIndex int64          // Index of highest log entry known to be committed
	lastApplied int64          // Index of highest log entry applied to state machine
	state       NodeState      // Current state of this node
	nodeID      string         // Unique identifier for this node
	peers       []string       // List of other nodes in cluster
	kvStore     *KeyValueStore // Key-value store
	voteCount   int            // Number of votes received in current election
}

// NewRaftNode creates a new RaftNode with the given nodeID and peers
func NewRaftNode(nodeID string, peers []string) *RaftNode {
	return &RaftNode{
		currentTerm: 0,
		votedFor:    "",
		log:         []LogEntry{},
		commitIndex: 0,
		lastApplied: 0,
		state:       Follower,
		nodeID:      nodeID,
		peers:       peers,
		kvStore:     NewKeyValueStore(),
		voteCount:   0,
	}
}

// GetCurrentTerm returns the current term of the node
func (n *RaftNode) GetCurrentTerm() int64 {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.currentTerm
}

// GetState returns the current state of the node
func (n *RaftNode) GetState() NodeState {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.state
}

// GetLogLength returns the length of the log
func (n *RaftNode) GetLogLength() int {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return len(n.log)
}

// UpdateTerm updates the current term if newTerm is higher
// Automatically becomes Follower and clears votedFor when term increases
func (n *RaftNode) UpdateTerm(newTerm int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if newTerm > n.currentTerm {
		n.currentTerm = newTerm
		n.state = Follower
		n.votedFor = ""
	}
}

// SetState changes the node's state
func (n *RaftNode) SetState(newState NodeState) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.state = newState
}

// Vote records a vote for candidateID in the current term
// Returns true if vote was cast, false if already voted this term
func (n *RaftNode) Vote(candidateID string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.votedFor == "" || n.votedFor == candidateID {
		n.votedFor = candidateID
		return true
	}
	return false
}

// AppendLogEntry adds a new entry to the log with the given term and command
// Sets the index automatically based on the current log length
func (n *RaftNode) AppendLogEntry(term int64, command interface{}) {
	n.mu.Lock()
	defer n.mu.Unlock()

	index := int64(len(n.log) + 1)
	entry := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	n.log = append(n.log, entry)
}

// RequestVoteRequest represents a request for votes from a candidate
type RequestVoteRequest struct {
	Term         int64  // Candidate's term
	CandidateID  string // Candidate requesting vote
	LastLogIndex int64  // Index of candidate's last log entry
	LastLogTerm  int64  // Term of candidate's last log entry
}

// RequestVoteResponse represents a response to a vote request
type RequestVoteResponse struct {
	Term        int64 // CurrentTerm, for candidate to update itself
	VoteGranted bool  // True means candidate received vote
}

// AppendEntriesRequest represents a request to append entries to the log
type AppendEntriesRequest struct {
	Term         int64      // Leader's term
	LeaderID     string     // So follower can redirect clients
	PrevLogIndex int64      // Index of log entry immediately preceding new ones
	PrevLogTerm  int64      // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store, empty for heartbeat
	LeaderCommit int64      // Leader's commitIndex
}

// AppendEntriesResponse represents a response to an append entries request
type AppendEntriesResponse struct {
	Term    int64 // CurrentTerm, for leader to update itself
	Success bool  // True if follower contained entry matching prevLogIndex and prevLogTerm
}

// HandleRequestVote handles a vote request from a candidate
// Returns true if vote is granted, false otherwise
func (n *RaftNode) HandleRequestVote(request RequestVoteRequest) RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	response := RequestVoteResponse{
		Term:        n.currentTerm,
		VoteGranted: false,
	}

	// If request term is less than current term, reject
	if request.Term < n.currentTerm {
		return response
	}

	// If request term is greater than current term, update term and become follower
	if request.Term > n.currentTerm {
		n.currentTerm = request.Term
		n.state = Follower
		n.votedFor = ""
		response.Term = n.currentTerm
	}

	// Check if we can vote for this candidate
	canVote := (n.votedFor == "" || n.votedFor == request.CandidateID) &&
		n.isLogUpToDate(request.LastLogIndex, request.LastLogTerm)

	if canVote {
		n.votedFor = request.CandidateID
		response.VoteGranted = true
	}

	return response
}

// HandleAppendEntries handles an append entries request from a leader
// Implements the AppendEntries RPC receiver logic from the Raft paper
func (n *RaftNode) HandleAppendEntries(request AppendEntriesRequest) AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	response := AppendEntriesResponse{
		Term:    n.currentTerm,
		Success: false,
	}

	// 1. Reply false if term < currentTerm (§5.1)
	if request.Term < n.currentTerm {
		return response
	}

	// 2. If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if request.Term > n.currentTerm {
		n.currentTerm = request.Term
		n.state = Follower
		n.votedFor = ""
		response.Term = n.currentTerm
	}

	// 3. Reset election timer (not implemented yet, but noted)
	// This would reset the election timeout timer

	// 4. Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if request.PrevLogIndex > 0 {
		// Check if we have an entry at prevLogIndex
		if request.PrevLogIndex > int64(len(n.log)) {
			// We don't have enough log entries
			return response
		}

		// Check if the term matches
		prevEntryIndex := int(request.PrevLogIndex - 1) // Convert to 0-based index
		if prevEntryIndex >= 0 && prevEntryIndex < len(n.log) {
			if n.log[prevEntryIndex].Term != request.PrevLogTerm {
				// Term doesn't match
				return response
			}
		}
	}

	// 5. If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	conflictIndex := -1
	for i, entry := range request.Entries {
		entryIndex := request.PrevLogIndex + int64(i) + 1
		existingIndex := int(entryIndex - 1) // Convert to 0-based index

		if existingIndex < len(n.log) {
			if n.log[existingIndex].Term != entry.Term {
				// Conflict found, truncate log from this point
				n.log = n.log[:existingIndex]
				conflictIndex = existingIndex
				break
			}
		} else {
			// No existing entry at this index, no conflict
			break
		}
	}

	// 6. Append any new entries not already in the log
	if len(request.Entries) > 0 {
		// Truncate log if there was a conflict
		if conflictIndex >= 0 {
			n.log = n.log[:conflictIndex]
		}

		// Append all new entries
		for _, entry := range request.Entries {
			// Set proper index for the entry
			entry.Index = int64(len(n.log) + 1)
			n.log = append(n.log, entry)
		}
	}

	// 7. If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if request.LeaderCommit > n.commitIndex {
		lastNewEntryIndex := int64(len(n.log))
		if request.LeaderCommit < lastNewEntryIndex {
			n.commitIndex = request.LeaderCommit
		} else {
			n.commitIndex = lastNewEntryIndex
		}

		// Apply newly committed entries to state machine
		for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
			entryIndex := int(i - 1)
			if entryIndex < len(n.log) {
				entry := n.log[entryIndex]
				if putCmd, ok := entry.Command.(PutCommand); ok {
					n.kvStore.Put(putCmd.Key, putCmd.Value)
				}
			}
		}
		n.lastApplied = n.commitIndex
	}

	response.Success = true
	return response
}

// isLogUpToDate checks if the candidate's log is at least as up-to-date as this node's log
// A log is more up-to-date if it has a higher term in its last entry,
// or if it has the same term but a higher index
func (n *RaftNode) isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm int64) bool {
	// If this node has no log entries, any candidate is up-to-date
	if len(n.log) == 0 {
		return true
	}

	// Get this node's last log entry
	lastEntry := n.log[len(n.log)-1]

	// Compare terms first
	if candidateLastLogTerm > lastEntry.Term {
		return true
	}

	if candidateLastLogTerm < lastEntry.Term {
		return false
	}

	// If terms are equal, compare indices
	return candidateLastLogIndex >= lastEntry.Index
}

// StartElection initiates a leader election
// Increments currentTerm, sets state to Candidate, and votes for itself
func (n *RaftNode) StartElection() int64 {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.nodeID
	n.voteCount = 1 // Vote for self

	return n.currentTerm
}

// SendRequestVote creates a RequestVoteRequest for the specified target node
// Returns the request struct (networking will be added later)
func (n *RaftNode) SendRequestVote(targetNodeID string) RequestVoteRequest {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Get last log entry info
	var lastLogIndex, lastLogTerm int64
	if len(n.log) > 0 {
		lastEntry := n.log[len(n.log)-1]
		lastLogIndex = lastEntry.Index
		lastLogTerm = lastEntry.Term
	}

	return RequestVoteRequest{
		Term:         n.currentTerm,
		CandidateID:  n.nodeID,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

// ProcessVoteResponse processes a vote response from a peer
// Returns true if this node should become leader (has majority votes)
func (n *RaftNode) ProcessVoteResponse(response RequestVoteResponse) bool {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Update term if response term is higher
	if response.Term > n.currentTerm {
		n.currentTerm = response.Term
		n.state = Follower
		n.votedFor = ""
		n.voteCount = 0
		return false // Cannot become leader if term was updated
	}

	// If we're not a candidate anymore, don't process vote
	if n.state != Candidate {
		return false
	}

	// Only process vote if it's for the current term
	if response.Term != n.currentTerm {
		return false
	}

	// If vote was granted, increment vote count
	if response.VoteGranted {
		n.voteCount++
	}

	// Return true if we have majority votes
	majorityCount := n.getMajorityCount()
	return n.voteCount >= majorityCount
}

// SendAppendEntries creates an AppendEntriesRequest for the specified target node
// Returns the request struct (networking will be added later)
func (n *RaftNode) SendAppendEntries(targetNodeID string, entries []LogEntry) AppendEntriesRequest {
	n.mu.RLock()
	defer n.mu.RUnlock()

	// Get previous log entry info
	var prevLogIndex, prevLogTerm int64
	if len(n.log) > 0 {
		lastEntry := n.log[len(n.log)-1]
		prevLogIndex = lastEntry.Index
		prevLogTerm = lastEntry.Term
	}

	return AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.nodeID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
}

// getMajorityCount returns the number of votes needed for majority
func (n *RaftNode) getMajorityCount() int {
	return (len(n.peers)+1)/2 + 1
}

// ClientGet retrieves a value from the key-value store
// Returns error if node is not the leader
func (n *RaftNode) ClientGet(key string) (string, bool, error) {
	n.mu.RLock()
	if n.state != Leader {
		n.mu.RUnlock()
		return "", false, errors.New("not leader")
	}
	n.mu.RUnlock()

	value, exists := n.kvStore.Get(key)
	return value, exists, nil
}

// ClientPut stores a key-value pair in the store
// Returns error if node is not the leader
func (n *RaftNode) ClientPut(key, value string) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return errors.New("not leader")
	}

	// Create a Put command
	putCommand := PutCommand{
		Key:   key,
		Value: value,
	}

	// Append to log directly (avoiding recursive lock)
	index := int64(len(n.log) + 1)
	entry := LogEntry{
		Term:    n.currentTerm,
		Index:   index,
		Command: putCommand,
	}
	n.log = append(n.log, entry)

	n.mu.Unlock()

	return nil
}

// UpdateCommitIndex updates the commit index and applies newly committed entries
func (n *RaftNode) UpdateCommitIndex(newCommitIndex int64) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Only update if newCommitIndex is greater than current commitIndex
	if newCommitIndex > n.commitIndex {
		n.commitIndex = newCommitIndex

		// Apply entries from lastApplied+1 to commitIndex directly
		for i := n.lastApplied + 1; i <= n.commitIndex; i++ {
			// Convert from 1-based index to 0-based slice index
			entryIndex := int(i - 1)
			if entryIndex < len(n.log) {
				entry := n.log[entryIndex]

				// Cast Command to PutCommand and apply
				if putCmd, ok := entry.Command.(PutCommand); ok {
					n.kvStore.Put(putCmd.Key, putCmd.Value)
				}
			}
		}

		// Update lastApplied
		n.lastApplied = n.commitIndex
	}
}
