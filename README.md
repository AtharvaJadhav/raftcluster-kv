# Raft Cluster Key-Value Store

This project implements the Raft consensus algorithm in Go for building a distributed key-value store.

## Project Structure

```
raftcluster-kv/
├── go.mod              # Go module definition
├── raft/
│   ├── raft_node.go    # Core Raft node implementation
│   └── raft_node_test.go # Tests for Raft node functionality
└── README.md           # This file
```

## Core Data Structures

### LogEntry
Represents a single entry in the Raft log:
- `Term`: Leadership term when entry was created
- `Index`: Position in log, starting from 1
- `Command`: The actual operation to execute

### NodeState
Enum representing the three possible states of a Raft node:
- `Follower`: Default state, responds to requests from leaders and candidates
- `Candidate`: Used during leader election
- `Leader`: Handles all client requests and log replication

### RaftNode
Main structure representing a single node in the Raft cluster:
- `mu`: Read-write mutex for thread safety
- `currentTerm`: Latest term this node has seen
- `votedFor`: Candidate that received vote in current term
- `log`: Sequence of log entries
- `commitIndex`: Index of highest log entry known to be committed
- `lastApplied`: Index of highest log entry applied to state machine
- `state`: Current state of this node
- `nodeID`: Unique identifier for this node
- `peers`: List of other nodes in cluster

## Basic Functionality

### Constructor
- `NewRaftNode(nodeID string, peers []string) *RaftNode`: Creates a new RaftNode initialized in Follower state

### Getter Methods (Thread-Safe)
- `GetCurrentTerm() int64`: Returns the current term
- `GetState() NodeState`: Returns the current state
- `GetLogLength() int`: Returns the length of the log

### State Management Methods (Thread-Safe)
- `UpdateTerm(newTerm int64)`: Updates currentTerm if newTerm is higher, automatically becomes Follower and clears votedFor
- `SetState(newState NodeState)`: Changes the node's state (Follower/Candidate/Leader)
- `Vote(candidateID string) bool`: Records vote for candidateID in current term, returns true if vote was cast
- `AppendLogEntry(term int64, command interface{})`: Adds new entry to log with given term and command, sets index automatically

### Leader Election Structures
- `RequestVoteRequest`: Contains candidate's term, ID, and log information
- `RequestVoteResponse`: Contains current term and vote grant status

### Leader Election Methods (Thread-Safe)
- `HandleRequestVote(request RequestVoteRequest) RequestVoteResponse`: Processes vote requests following Raft protocol
- `isLogUpToDate(candidateLastLogIndex, candidateLastLogTerm int64) bool`: Compares log currency between nodes
- `StartElection() int64`: Initiates leader election, increments term, becomes candidate, votes for self
- `SendRequestVote(targetNodeID string) RequestVoteRequest`: Creates vote request for target node
- `ProcessVoteResponse(response RequestVoteResponse) bool`: Processes vote response, returns true if should become leader
- `getMajorityCount() int`: Returns number of votes needed for majority

### Log Replication Structures
- `AppendEntriesRequest`: Contains leader's term, ID, log entries, and commit information
- `AppendEntriesResponse`: Contains current term and success status

### Log Replication Methods (Thread-Safe)
- `HandleAppendEntries(request AppendEntriesRequest) AppendEntriesResponse`: Processes log replication requests following Raft protocol
- `SendAppendEntries(targetNodeID string, entries []LogEntry) AppendEntriesRequest`: Creates append entries request for target node

### Key-Value Store
- `KeyValueStore`: Simple thread-safe key-value storage with Get/Put operations
- `NewKeyValueStore()`: Creates a new empty key-value store

### Client Operations (Thread-Safe)
- `ClientGet(key string) (string, bool, error)`: Retrieves value from store, returns error if not leader
- `ClientPut(key, value string) error`: Stores key-value pair in log, returns error if not leader

### State Machine Operations (Thread-Safe)
- `PutCommand`: Struct representing a put operation with Key and Value fields
- `UpdateCommitIndex(newCommitIndex int64)`: Updates commit index and applies newly committed log entries to state machine

## Running Tests

```bash
go test ./raft
```

## Next Steps

The core data structures are now in place. The next phase will involve implementing:
1. Raft protocol methods (RequestVote, AppendEntries)
2. Leader election logic
3. Log replication
4. State machine application
5. Network communication layer 