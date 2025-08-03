# Raft Key-Value Store

A simple Raft consensus implementation in Go.

## Run

```bash
# Demo
go run main.go election_demo.go

# Tests
go test ./raft
```

## What it does

- Leader election with vote counting
- Log replication between nodes
- Key-value store with Get/Put operations
- Thread-safe operations

## Structure

```
raft/
├── raft_node.go      # Core Raft implementation
└── raft_node_test.go # Tests
```

## Example

```go
node := NewRaftNode("node1", []string{"node2", "node3"})
node.SetState(Leader)
node.ClientPut("key", "value")
value, exists, _ := node.ClientGet("key")
``` 