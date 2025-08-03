# Raft Cluster Key-Value Store

A simple implementation of the Raft consensus algorithm in Go for building a distributed key-value store.

## What it does

This project implements the core Raft consensus protocol that allows multiple servers to agree on a sequence of operations even when some fail. Each server can be in one of three states:

- **Follower**: Default state, responds to requests from leaders
- **Candidate**: Used during leader election
- **Leader**: Handles client requests and replicates logs to followers

The system provides a basic key-value store interface where only the leader can handle client operations. Commands are first added to the leader's log, then replicated to followers, and finally applied to the state machine when committed.

## Quick Start

```bash
# Run all tests
go test ./raft

# Run tests with verbose output
go test -v ./raft

# Run specific test
go test -run TestVoteCountingMechanism ./raft

# Run the interactive demo
go run main.go election_demo.go
```

## Project Structure

```
raftcluster-kv/
├── go.mod              # Go module definition
├── raft/
│   ├── raft_node.go    # Core Raft implementation
│   └── raft_node_test.go # Tests
└── README.md           # This file
```

## Key Features

- **Leader Election**: Automatic leader election with proper vote counting
- **Log Replication**: Leaders replicate log entries to followers
- **State Machine**: Committed log entries are applied to key-value store
- **Thread Safety**: All operations are thread-safe for concurrent access
- **Client Interface**: Simple Get/Put operations (leader-only)

## Example Usage

```go
// Create a new Raft node
node := NewRaftNode("node1", []string{"node2", "node3"})

// Make it leader (in real system, this happens through election)
node.SetState(Leader)

// Store a key-value pair
err := node.ClientPut("hello", "world")
if err != nil {
    log.Fatal(err)
}

// Retrieve the value
value, exists, err := node.ClientGet("hello")
if err != nil {
    log.Fatal(err)
}
fmt.Printf("Value: %s, Exists: %t\n", value, exists)
```

## Testing

The test suite covers all major components:

- Basic data structures and state management
- Leader election and vote counting
- Log replication and conflict resolution
- Client operations and state machine application
- Thread safety and concurrent access

Run `go test -v ./raft` to see all test cases.

## Next Steps

This is a core implementation focused on the Raft algorithm. To make it production-ready, you'd need to add:

- Network communication between nodes
- Election timeouts and heartbeat mechanisms
- Persistence of log entries and state
- Client request handling and response management
- Cluster membership management 