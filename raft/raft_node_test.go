package raft

import (
	"testing"
)

func TestRaftDataStructures(t *testing.T) {
	// Test LogEntry creation
	entry := LogEntry{
		Term:    1,
		Index:   1,
		Command: "test command",
	}

	if entry.Term != 1 || entry.Index != 1 || entry.Command != "test command" {
		t.Errorf("LogEntry fields not set correctly")
	}

	// Test NodeState enum values
	if Follower != 0 || Candidate != 1 || Leader != 2 {
		t.Errorf("NodeState enum values not correct")
	}

	// Test RaftNode creation
	node := RaftNode{
		currentTerm: 0,
		votedFor:    "",
		log:         []LogEntry{},
		commitIndex: 0,
		lastApplied: 0,
		state:       Follower,
		nodeID:      "node1",
		peers:       []string{"node2", "node3"},
	}

	if node.currentTerm != 0 || node.state != Follower || node.nodeID != "node1" {
		t.Errorf("RaftNode fields not set correctly")
	}

	if len(node.peers) != 2 {
		t.Errorf("RaftNode peers not set correctly")
	}
}

func TestNewRaftNode(t *testing.T) {
	peers := []string{"node2", "node3"}
	node := NewRaftNode("node1", peers)

	if node.nodeID != "node1" {
		t.Errorf("Expected nodeID to be 'node1', got %s", node.nodeID)
	}

	if len(node.peers) != 2 {
		t.Errorf("Expected 2 peers, got %d", len(node.peers))
	}

	if node.currentTerm != 0 {
		t.Errorf("Expected currentTerm to be 0, got %d", node.currentTerm)
	}

	if node.votedFor != "" {
		t.Errorf("Expected votedFor to be empty, got %s", node.votedFor)
	}

	if len(node.log) != 0 {
		t.Errorf("Expected empty log, got log with %d entries", len(node.log))
	}

	if node.commitIndex != 0 {
		t.Errorf("Expected commitIndex to be 0, got %d", node.commitIndex)
	}

	if node.lastApplied != 0 {
		t.Errorf("Expected lastApplied to be 0, got %d", node.lastApplied)
	}

	if node.state != Follower {
		t.Errorf("Expected state to be Follower, got %v", node.state)
	}
}

func TestGetterMethods(t *testing.T) {
	peers := []string{"node2", "node3"}
	node := NewRaftNode("node1", peers)

	// Test GetCurrentTerm
	if term := node.GetCurrentTerm(); term != 0 {
		t.Errorf("Expected GetCurrentTerm to return 0, got %d", term)
	}

	// Test GetState
	if state := node.GetState(); state != Follower {
		t.Errorf("Expected GetState to return Follower, got %v", state)
	}

	// Test GetLogLength
	if length := node.GetLogLength(); length != 0 {
		t.Errorf("Expected GetLogLength to return 0, got %d", length)
	}
}

func TestUpdateTerm(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Test updating to higher term
	node.UpdateTerm(5)
	if term := node.GetCurrentTerm(); term != 5 {
		t.Errorf("Expected currentTerm to be 5, got %d", term)
	}
	if state := node.GetState(); state != Follower {
		t.Errorf("Expected state to be Follower, got %v", state)
	}

	// Test that lower term doesn't update
	node.UpdateTerm(3)
	if term := node.GetCurrentTerm(); term != 5 {
		t.Errorf("Expected currentTerm to remain 5, got %d", term)
	}

	// Test that same term doesn't update
	node.UpdateTerm(5)
	if term := node.GetCurrentTerm(); term != 5 {
		t.Errorf("Expected currentTerm to remain 5, got %d", term)
	}
}

func TestSetState(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Test setting to Candidate
	node.SetState(Candidate)
	if state := node.GetState(); state != Candidate {
		t.Errorf("Expected state to be Candidate, got %v", state)
	}

	// Test setting to Leader
	node.SetState(Leader)
	if state := node.GetState(); state != Leader {
		t.Errorf("Expected state to be Leader, got %v", state)
	}

	// Test setting back to Follower
	node.SetState(Follower)
	if state := node.GetState(); state != Follower {
		t.Errorf("Expected state to be Follower, got %v", state)
	}
}

func TestVote(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Test first vote
	if !node.Vote("candidate1") {
		t.Errorf("Expected first vote to succeed")
	}

	// Test voting for same candidate again
	if !node.Vote("candidate1") {
		t.Errorf("Expected voting for same candidate to succeed")
	}

	// Test voting for different candidate (should fail)
	if node.Vote("candidate2") {
		t.Errorf("Expected voting for different candidate to fail")
	}
}

func TestAppendLogEntry(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Test appending first entry
	node.AppendLogEntry(1, "command1")
	if length := node.GetLogLength(); length != 1 {
		t.Errorf("Expected log length to be 1, got %d", length)
	}

	// Test appending second entry
	node.AppendLogEntry(1, "command2")
	if length := node.GetLogLength(); length != 2 {
		t.Errorf("Expected log length to be 2, got %d", length)
	}

	// Test appending entry with different term
	node.AppendLogEntry(2, "command3")
	if length := node.GetLogLength(); length != 3 {
		t.Errorf("Expected log length to be 3, got %d", length)
	}
}

func TestHandleRequestVote(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2", "node3"})

	// Test vote request with same term (should grant if log is up-to-date)
	request := RequestVoteRequest{
		Term:         0,
		CandidateID:  "candidate1",
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	response := node.HandleRequestVote(request)
	if !response.VoteGranted {
		t.Errorf("Expected vote to be granted for same term with up-to-date log")
	}
	if response.Term != 0 {
		t.Errorf("Expected response term to be 0, got %d", response.Term)
	}

	// Test vote request with lower term (should reject)
	node.UpdateTerm(5)
	request = RequestVoteRequest{
		Term:         3,
		CandidateID:  "candidate2",
		LastLogIndex: 1,
		LastLogTerm:  1,
	}
	response = node.HandleRequestVote(request)
	if response.VoteGranted {
		t.Errorf("Expected vote to be rejected for lower term")
	}
	if response.Term != 5 {
		t.Errorf("Expected response term to be 5, got %d", response.Term)
	}

	// Test vote request with higher term (should update term and potentially grant vote)
	request = RequestVoteRequest{
		Term:         7,
		CandidateID:  "candidate3",
		LastLogIndex: 1,
		LastLogTerm:  1,
	}
	response = node.HandleRequestVote(request)
	if !response.VoteGranted {
		t.Errorf("Expected vote to be granted for higher term")
	}
	if response.Term != 7 {
		t.Errorf("Expected response term to be 7, got %d", response.Term)
	}
	if node.GetCurrentTerm() != 7 {
		t.Errorf("Expected node term to be updated to 7, got %d", node.GetCurrentTerm())
	}
	if node.GetState() != Follower {
		t.Errorf("Expected node to become follower, got %v", node.GetState())
	}

	// Test vote request from different candidate in same term (should reject)
	request = RequestVoteRequest{
		Term:         7,
		CandidateID:  "candidate4",
		LastLogIndex: 1,
		LastLogTerm:  1,
	}
	response = node.HandleRequestVote(request)
	if response.VoteGranted {
		t.Errorf("Expected vote to be rejected for different candidate in same term")
	}

	// Test vote request from same candidate in same term (should grant)
	request = RequestVoteRequest{
		Term:         7,
		CandidateID:  "candidate3",
		LastLogIndex: 1,
		LastLogTerm:  1,
	}
	response = node.HandleRequestVote(request)
	if !response.VoteGranted {
		t.Errorf("Expected vote to be granted for same candidate in same term")
	}
}

func TestIsLogUpToDate(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Test with empty log (any candidate should be up-to-date)
	if !node.isLogUpToDate(0, 0) {
		t.Errorf("Expected empty log to consider any candidate up-to-date")
	}

	// Add some log entries
	node.AppendLogEntry(1, "command1")
	node.AppendLogEntry(1, "command2")
	node.AppendLogEntry(2, "command3")

	// Test candidate with higher term
	if !node.isLogUpToDate(1, 3) {
		t.Errorf("Expected candidate with higher term to be up-to-date")
	}

	// Test candidate with lower term
	if node.isLogUpToDate(1, 1) {
		t.Errorf("Expected candidate with lower term to not be up-to-date")
	}

	// Test candidate with same term but higher index
	if !node.isLogUpToDate(4, 2) {
		t.Errorf("Expected candidate with same term but higher index to be up-to-date")
	}

	// Test candidate with same term but lower index
	if node.isLogUpToDate(2, 2) {
		t.Errorf("Expected candidate with same term but lower index to not be up-to-date")
	}

	// Test candidate with same term and same index
	if !node.isLogUpToDate(3, 2) {
		t.Errorf("Expected candidate with same term and same index to be up-to-date")
	}
}

func TestRequestVoteStructures(t *testing.T) {
	// Test RequestVoteRequest
	request := RequestVoteRequest{
		Term:         5,
		CandidateID:  "candidate1",
		LastLogIndex: 10,
		LastLogTerm:  3,
	}

	if request.Term != 5 {
		t.Errorf("Expected Term to be 5, got %d", request.Term)
	}
	if request.CandidateID != "candidate1" {
		t.Errorf("Expected CandidateID to be 'candidate1', got %s", request.CandidateID)
	}
	if request.LastLogIndex != 10 {
		t.Errorf("Expected LastLogIndex to be 10, got %d", request.LastLogIndex)
	}
	if request.LastLogTerm != 3 {
		t.Errorf("Expected LastLogTerm to be 3, got %d", request.LastLogTerm)
	}

	// Test RequestVoteResponse
	response := RequestVoteResponse{
		Term:        5,
		VoteGranted: true,
	}

	if response.Term != 5 {
		t.Errorf("Expected Term to be 5, got %d", response.Term)
	}
	if !response.VoteGranted {
		t.Errorf("Expected VoteGranted to be true")
	}
}

func TestStartElection(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2", "node3"})

	// Test initial state
	if node.GetCurrentTerm() != 0 {
		t.Errorf("Expected initial term to be 0, got %d", node.GetCurrentTerm())
	}
	if node.GetState() != Follower {
		t.Errorf("Expected initial state to be Follower, got %v", node.GetState())
	}

	// Start election
	newTerm := node.StartElection()
	if newTerm != 1 {
		t.Errorf("Expected new term to be 1, got %d", newTerm)
	}

	// Check state changes
	if node.GetCurrentTerm() != 1 {
		t.Errorf("Expected current term to be 1, got %d", node.GetCurrentTerm())
	}
	if node.GetState() != Candidate {
		t.Errorf("Expected state to be Candidate, got %v", node.GetState())
	}

	// Start another election
	newTerm = node.StartElection()
	if newTerm != 2 {
		t.Errorf("Expected new term to be 2, got %d", newTerm)
	}
	if node.GetCurrentTerm() != 2 {
		t.Errorf("Expected current term to be 2, got %d", node.GetCurrentTerm())
	}
}

func TestSendRequestVote(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2", "node3"})

	// Test with empty log
	request := node.SendRequestVote("node2")
	if request.Term != 0 {
		t.Errorf("Expected term to be 0, got %d", request.Term)
	}
	if request.CandidateID != "node1" {
		t.Errorf("Expected candidate ID to be 'node1', got %s", request.CandidateID)
	}
	if request.LastLogIndex != 0 {
		t.Errorf("Expected last log index to be 0, got %d", request.LastLogIndex)
	}
	if request.LastLogTerm != 0 {
		t.Errorf("Expected last log term to be 0, got %d", request.LastLogTerm)
	}

	// Add some log entries and test again
	node.AppendLogEntry(1, "command1")
	node.AppendLogEntry(2, "command2")

	request = node.SendRequestVote("node3")
	if request.Term != 0 {
		t.Errorf("Expected term to be 0, got %d", request.Term)
	}
	if request.CandidateID != "node1" {
		t.Errorf("Expected candidate ID to be 'node1', got %s", request.CandidateID)
	}
	if request.LastLogIndex != 2 {
		t.Errorf("Expected last log index to be 2, got %d", request.LastLogIndex)
	}
	if request.LastLogTerm != 2 {
		t.Errorf("Expected last log term to be 2, got %d", request.LastLogTerm)
	}

	// Test after starting election
	node.StartElection()
	request = node.SendRequestVote("node2")
	if request.Term != 1 {
		t.Errorf("Expected term to be 1, got %d", request.Term)
	}
}

func TestProcessVoteResponse(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2", "node3"})

	// Test when not a candidate (should return false)
	response := RequestVoteResponse{Term: 0, VoteGranted: true}
	if node.ProcessVoteResponse(response) {
		t.Errorf("Expected false when not a candidate")
	}

	// Start election to become candidate
	node.StartElection()

	// Test with higher term response (should become follower)
	response = RequestVoteResponse{Term: 5, VoteGranted: false}
	if node.ProcessVoteResponse(response) {
		t.Errorf("Expected false when term is updated")
	}
	if node.GetCurrentTerm() != 5 {
		t.Errorf("Expected term to be updated to 5, got %d", node.GetCurrentTerm())
	}
	if node.GetState() != Follower {
		t.Errorf("Expected state to be Follower, got %v", node.GetState())
	}

	// Start election again
	node.StartElection()

	// Test with same term response
	response = RequestVoteResponse{Term: 6, VoteGranted: true}
	result := node.ProcessVoteResponse(response)
	// The result depends on the simplified logic - just check it doesn't crash
	_ = result
}

func TestGetMajorityCount(t *testing.T) {
	// Test with 2 peers (total 3 nodes)
	node := NewRaftNode("node1", []string{"node2", "node3"})
	majority := node.getMajorityCount()
	if majority != 2 {
		t.Errorf("Expected majority count to be 2 for 3 total nodes, got %d", majority)
	}

	// Test with 3 peers (total 4 nodes)
	node = NewRaftNode("node1", []string{"node2", "node3", "node4"})
	majority = node.getMajorityCount()
	if majority != 3 {
		t.Errorf("Expected majority count to be 3 for 4 total nodes, got %d", majority)
	}

	// Test with 4 peers (total 5 nodes)
	node = NewRaftNode("node1", []string{"node2", "node3", "node4", "node5"})
	majority = node.getMajorityCount()
	if majority != 3 {
		t.Errorf("Expected majority count to be 3 for 5 total nodes, got %d", majority)
	}

	// Test with 1 peer (total 2 nodes)
	node = NewRaftNode("node1", []string{"node2"})
	majority = node.getMajorityCount()
	if majority != 2 {
		t.Errorf("Expected majority count to be 2 for 2 total nodes, got %d", majority)
	}
}

func TestKeyValueStore(t *testing.T) {
	kv := NewKeyValueStore()

	// Test initial state
	if len(kv.data) != 0 {
		t.Errorf("Expected empty key-value store")
	}

	// Test Put and Get
	kv.Put("key1", "value1")
	value, exists := kv.Get("key1")
	if !exists {
		t.Errorf("Expected key1 to exist")
	}
	if value != "value1" {
		t.Errorf("Expected value to be 'value1', got %s", value)
	}

	// Test Get for non-existent key
	_, exists = kv.Get("nonexistent")
	if exists {
		t.Errorf("Expected key to not exist")
	}

	// Test overwriting
	kv.Put("key1", "newvalue")
	value, exists = kv.Get("key1")
	if !exists {
		t.Errorf("Expected key1 to exist after overwrite")
	}
	if value != "newvalue" {
		t.Errorf("Expected value to be 'newvalue', got %s", value)
	}

	// Test multiple keys
	kv.Put("key2", "value2")
	value, exists = kv.Get("key2")
	if !exists {
		t.Errorf("Expected key2 to exist")
	}
	if value != "value2" {
		t.Errorf("Expected value to be 'value2', got %s", value)
	}
}

func TestClientOperations(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2", "node3"})

	// Test ClientGet when not leader (should fail)
	_, _, err := node.ClientGet("key1")
	if err == nil {
		t.Errorf("Expected error when not leader")
	}
	if err.Error() != "not leader" {
		t.Errorf("Expected 'not leader' error, got %s", err.Error())
	}

	// Test ClientPut when not leader (should fail)
	err = node.ClientPut("key1", "value1")
	if err == nil {
		t.Errorf("Expected error when not leader")
	}
	if err.Error() != "not leader" {
		t.Errorf("Expected 'not leader' error, got %s", err.Error())
	}

	// Make node leader
	node.SetState(Leader)

	// Test ClientGet when leader (should succeed)
	_, exists, err := node.ClientGet("key1")
	if err != nil {
		t.Errorf("Expected no error when leader")
	}
	if exists {
		t.Errorf("Expected key to not exist initially")
	}

	// Test ClientPut when leader (should succeed)
	err = node.ClientPut("key1", "value1")
	if err != nil {
		t.Errorf("Expected no error when leader")
	}

	// Verify log entry was added
	if node.GetLogLength() != 1 {
		t.Errorf("Expected log length to be 1, got %d", node.GetLogLength())
	}

	// Test ClientGet after put (should not return the value yet - not committed)
	_, exists, err = node.ClientGet("key1")
	if err != nil {
		t.Errorf("Expected no error when leader")
	}
	if exists {
		t.Errorf("Expected key to not exist yet - command not committed")
	}

	// Test multiple operations
	err = node.ClientPut("key2", "value2")
	if err != nil {
		t.Errorf("Expected no error for second put")
	}

	_, exists, err = node.ClientGet("key2")
	if err != nil {
		t.Errorf("Expected no error for second get")
	}
	if exists {
		t.Errorf("Expected key2 to not exist yet - command not committed")
	}

	// Verify log length increased
	if node.GetLogLength() != 2 {
		t.Errorf("Expected log length to be 2, got %d", node.GetLogLength())
	}
}

func TestNewRaftNodeWithKVStore(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Verify kvStore was initialized
	if node.kvStore == nil {
		t.Errorf("Expected kvStore to be initialized")
	}

	// Test that kvStore is empty initially
	value, exists := node.kvStore.Get("test")
	if exists {
		t.Errorf("Expected kvStore to be empty initially")
	}
	if value != "" {
		t.Errorf("Expected empty value, got %s", value)
	}
}

func TestPutCommand(t *testing.T) {
	// Test PutCommand struct
	cmd := PutCommand{
		Key:   "testkey",
		Value: "testvalue",
	}

	if cmd.Key != "testkey" {
		t.Errorf("Expected Key to be 'testkey', got %s", cmd.Key)
	}
	if cmd.Value != "testvalue" {
		t.Errorf("Expected Value to be 'testvalue', got %s", cmd.Value)
	}
}

func TestUpdateCommitIndexWithLogApplication(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Add some log entries
	node.AppendLogEntry(1, PutCommand{Key: "key1", Value: "value1"})
	node.AppendLogEntry(1, PutCommand{Key: "key2", Value: "value2"})

	// Initially, no entries should be applied
	value, exists := node.kvStore.Get("key1")
	if exists {
		t.Errorf("Expected key1 to not exist before applying")
	}

	// Update commit index to apply entries
	node.UpdateCommitIndex(2)

	// Now entries should be applied
	value, exists = node.kvStore.Get("key1")
	if !exists {
		t.Errorf("Expected key1 to exist after applying")
	}
	if value != "value1" {
		t.Errorf("Expected value1, got %s", value)
	}

	value, exists = node.kvStore.Get("key2")
	if !exists {
		t.Errorf("Expected key2 to exist after applying")
	}
	if value != "value2" {
		t.Errorf("Expected value2, got %s", value)
	}
}

func TestUpdateCommitIndex(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Initial commit index should be 0
	if node.commitIndex != 0 {
		t.Errorf("Expected initial commitIndex to be 0, got %d", node.commitIndex)
	}

	// Update to higher value
	node.UpdateCommitIndex(5)
	if node.commitIndex != 5 {
		t.Errorf("Expected commitIndex to be 5, got %d", node.commitIndex)
	}

	// Update to lower value (should not change)
	node.UpdateCommitIndex(3)
	if node.commitIndex != 5 {
		t.Errorf("Expected commitIndex to remain 5, got %d", node.commitIndex)
	}

	// Update to same value (should not change)
	node.UpdateCommitIndex(5)
	if node.commitIndex != 5 {
		t.Errorf("Expected commitIndex to remain 5, got %d", node.commitIndex)
	}
}

func TestClientPutWithStateMachine(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})
	node.SetState(Leader)

	// Put a value
	err := node.ClientPut("testkey", "testvalue")
	if err != nil {
		t.Errorf("Expected no error for ClientPut")
	}

	// Value should not exist yet (not committed)
	value, exists := node.kvStore.Get("testkey")
	if exists {
		t.Errorf("Expected key to not exist before commit")
	}

	// Commit the entry
	node.UpdateCommitIndex(1)

	// Now value should exist
	value, exists = node.kvStore.Get("testkey")
	if !exists {
		t.Errorf("Expected key to exist after commit")
	}
	if value != "testvalue" {
		t.Errorf("Expected 'testvalue', got %s", value)
	}

	// Test multiple operations
	node.ClientPut("key2", "value2")
	node.ClientPut("key3", "value3")

	// Commit all entries
	node.UpdateCommitIndex(3)

	// Check all values
	value, exists = node.kvStore.Get("key2")
	if !exists || value != "value2" {
		t.Errorf("Expected key2=value2")
	}

	value, exists = node.kvStore.Get("key3")
	if !exists || value != "value3" {
		t.Errorf("Expected key3=value3")
	}
}

func TestAppendEntriesStructures(t *testing.T) {
	// Test AppendEntriesRequest
	request := AppendEntriesRequest{
		Term:         5,
		LeaderID:     "leader1",
		PrevLogIndex: 10,
		PrevLogTerm:  3,
		Entries:      []LogEntry{},
		LeaderCommit: 8,
	}

	if request.Term != 5 {
		t.Errorf("Expected Term to be 5, got %d", request.Term)
	}
	if request.LeaderID != "leader1" {
		t.Errorf("Expected LeaderID to be 'leader1', got %s", request.LeaderID)
	}
	if request.PrevLogIndex != 10 {
		t.Errorf("Expected PrevLogIndex to be 10, got %d", request.PrevLogIndex)
	}
	if request.PrevLogTerm != 3 {
		t.Errorf("Expected PrevLogTerm to be 3, got %d", request.PrevLogTerm)
	}
	if request.LeaderCommit != 8 {
		t.Errorf("Expected LeaderCommit to be 8, got %d", request.LeaderCommit)
	}

	// Test AppendEntriesResponse
	response := AppendEntriesResponse{
		Term:    5,
		Success: true,
	}

	if response.Term != 5 {
		t.Errorf("Expected Term to be 5, got %d", response.Term)
	}
	if !response.Success {
		t.Errorf("Expected Success to be true")
	}
}

func TestHandleAppendEntries(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Test 1: Reply false if term < currentTerm
	node.UpdateTerm(5)
	request := AppendEntriesRequest{
		Term:         3,
		LeaderID:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	response := node.HandleAppendEntries(request)
	if response.Success {
		t.Errorf("Expected failure for lower term")
	}
	if response.Term != 5 {
		t.Errorf("Expected response term to be 5, got %d", response.Term)
	}

	// Test 2: Update term if request term is higher
	request = AppendEntriesRequest{
		Term:         7,
		LeaderID:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	response = node.HandleAppendEntries(request)
	if !response.Success {
		t.Errorf("Expected success for higher term")
	}
	if response.Term != 7 {
		t.Errorf("Expected response term to be 7, got %d", response.Term)
	}
	if node.GetCurrentTerm() != 7 {
		t.Errorf("Expected node term to be updated to 7, got %d", node.GetCurrentTerm())
	}
	if node.GetState() != Follower {
		t.Errorf("Expected node to become follower, got %v", node.GetState())
	}

	// Test 3: Heartbeat (empty entries) should succeed
	request = AppendEntriesRequest{
		Term:         7,
		LeaderID:     "leader1",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []LogEntry{},
		LeaderCommit: 0,
	}
	response = node.HandleAppendEntries(request)
	if !response.Success {
		t.Errorf("Expected heartbeat to succeed")
	}
}

func TestHandleAppendEntriesWithLogReplication(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Add some initial log entries
	node.AppendLogEntry(1, PutCommand{Key: "key1", Value: "value1"})
	node.AppendLogEntry(1, PutCommand{Key: "key2", Value: "value2"})

	// Test: Append new entries
	newEntries := []LogEntry{
		{Term: 1, Index: 3, Command: PutCommand{Key: "key3", Value: "value3"}},
		{Term: 1, Index: 4, Command: PutCommand{Key: "key4", Value: "value4"}},
	}

	request := AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader1",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      newEntries,
		LeaderCommit: 4,
	}

	response := node.HandleAppendEntries(request)
	if !response.Success {
		t.Errorf("Expected log replication to succeed")
	}

	// Verify log entries were added
	if node.GetLogLength() != 4 {
		t.Errorf("Expected log length to be 4, got %d", node.GetLogLength())
	}

	// Verify commit index was updated
	if node.commitIndex != 4 {
		t.Errorf("Expected commit index to be 4, got %d", node.commitIndex)
	}

	// Verify entries were applied to state machine
	value, exists := node.kvStore.Get("key3")
	if !exists {
		t.Errorf("Expected key3 to exist after replication")
	}
	if value != "value3" {
		t.Errorf("Expected value3, got %s", value)
	}

	value, exists = node.kvStore.Get("key4")
	if !exists {
		t.Errorf("Expected key4 to exist after replication")
	}
	if value != "value4" {
		t.Errorf("Expected value4, got %s", value)
	}
}

func TestHandleAppendEntriesLogConflict(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Add initial log entries
	node.AppendLogEntry(1, PutCommand{Key: "key1", Value: "value1"})
	node.AppendLogEntry(1, PutCommand{Key: "key2", Value: "value2"})
	node.AppendLogEntry(2, PutCommand{Key: "key3", Value: "value3"}) // Different term

	// Test: Append entries that conflict with existing log
	newEntries := []LogEntry{
		{Term: 1, Index: 3, Command: PutCommand{Key: "key3", Value: "newvalue3"}}, // Same index, different term
		{Term: 1, Index: 4, Command: PutCommand{Key: "key4", Value: "value4"}},
	}

	request := AppendEntriesRequest{
		Term:         1,
		LeaderID:     "leader1",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      newEntries,
		LeaderCommit: 4,
	}

	response := node.HandleAppendEntries(request)
	if !response.Success {
		t.Errorf("Expected log replication to succeed despite conflict")
	}

	// Verify log was truncated and new entries were added
	if node.GetLogLength() != 4 {
		t.Errorf("Expected log length to be 4 after conflict resolution, got %d", node.GetLogLength())
	}

	// Verify the conflicting entry was replaced
	value, exists := node.kvStore.Get("key3")
	if !exists {
		t.Errorf("Expected key3 to exist after conflict resolution")
	}
	if value != "newvalue3" {
		t.Errorf("Expected newvalue3, got %s", value)
	}
}

func TestVoteCountingMechanism(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2", "node3"})

	// Test initial vote count
	if node.voteCount != 0 {
		t.Errorf("Expected initial vote count to be 0, got %d", node.voteCount)
	}

	// Start election
	node.StartElection()
	if node.voteCount != 1 {
		t.Errorf("Expected vote count to be 1 after starting election, got %d", node.voteCount)
	}

	// Test vote counting
	response1 := RequestVoteResponse{Term: 1, VoteGranted: true}
	if !node.ProcessVoteResponse(response1) {
		t.Errorf("Expected to become leader with 2 votes (majority of 3)")
	}

	// Test vote counting with rejection
	node.StartElection() // Reset
	response2 := RequestVoteResponse{Term: 2, VoteGranted: false}
	if node.ProcessVoteResponse(response2) {
		t.Errorf("Expected not to become leader with only 1 vote")
	}

	// Test term update resets vote count
	response3 := RequestVoteResponse{Term: 5, VoteGranted: true}
	node.ProcessVoteResponse(response3)
	if node.voteCount != 0 {
		t.Errorf("Expected vote count to be reset to 0 after term update, got %d", node.voteCount)
	}
	if node.GetState() != Follower {
		t.Errorf("Expected to become follower after term update")
	}
}

func TestSendAppendEntries(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Test with empty log
	request := node.SendAppendEntries("node2", []LogEntry{})
	if request.Term != 0 {
		t.Errorf("Expected term to be 0, got %d", request.Term)
	}
	if request.LeaderID != "node1" {
		t.Errorf("Expected LeaderID to be 'node1', got %s", request.LeaderID)
	}
	if request.PrevLogIndex != 0 {
		t.Errorf("Expected PrevLogIndex to be 0, got %d", request.PrevLogIndex)
	}
	if request.PrevLogTerm != 0 {
		t.Errorf("Expected PrevLogTerm to be 0, got %d", request.PrevLogTerm)
	}
	if request.LeaderCommit != 0 {
		t.Errorf("Expected LeaderCommit to be 0, got %d", request.LeaderCommit)
	}

	// Add some log entries and test again
	node.AppendLogEntry(1, PutCommand{Key: "key1", Value: "value1"})
	node.AppendLogEntry(2, PutCommand{Key: "key2", Value: "value2"})

	entries := []LogEntry{
		{Term: 2, Index: 3, Command: PutCommand{Key: "key3", Value: "value3"}},
	}

	request = node.SendAppendEntries("node2", entries)
	if request.Term != 0 {
		t.Errorf("Expected term to be 0, got %d", request.Term)
	}
	if request.LeaderID != "node1" {
		t.Errorf("Expected LeaderID to be 'node1', got %s", request.LeaderID)
	}
	if request.PrevLogIndex != 2 {
		t.Errorf("Expected PrevLogIndex to be 2, got %d", request.PrevLogIndex)
	}
	if request.PrevLogTerm != 2 {
		t.Errorf("Expected PrevLogTerm to be 2, got %d", request.PrevLogTerm)
	}
	if request.LeaderCommit != 0 {
		t.Errorf("Expected LeaderCommit to be 0, got %d", request.LeaderCommit)
	}
	if len(request.Entries) != 1 {
		t.Errorf("Expected 1 entry, got %d", len(request.Entries))
	}

	// Test after becoming leader
	node.SetState(Leader)
	node.UpdateCommitIndex(2)
	request = node.SendAppendEntries("node2", entries)
	if request.LeaderCommit != 2 {
		t.Errorf("Expected LeaderCommit to be 2, got %d", request.LeaderCommit)
	}
}

func TestNewRaftNodeWithVoteCount(t *testing.T) {
	node := NewRaftNode("node1", []string{"node2"})

	// Verify voteCount was initialized
	if node.voteCount != 0 {
		t.Errorf("Expected initial voteCount to be 0, got %d", node.voteCount)
	}
}
