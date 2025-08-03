package main

import (
	"fmt"
	"log"
	"raftcluster-kv/raft"
	"strings"
)

func main() {
	fmt.Println("Raft Key-Value Store Demo")
	fmt.Println("=========================")

	// Create a new Raft node
	node := raft.NewRaftNode("node1", []string{"node2", "node3"})

	fmt.Printf("Created node: node1\n")
	fmt.Printf("Initial state: %v\n", node.GetState())
	fmt.Printf("Initial term: %d\n", node.GetCurrentTerm())

	// Make it leader (in a real system, this would happen through election)
	fmt.Println("\nMaking node leader...")
	node.SetState(raft.Leader)
	fmt.Printf("State: %v\n", node.GetState())

	// Store some key-value pairs
	fmt.Println("\nStoring key-value pairs...")

	err := node.ClientPut("name", "Alice")
	if err != nil {
		log.Fatal("Failed to put 'name':", err)
	}
	fmt.Println("✓ Stored: name = Alice")

	err = node.ClientPut("age", "30")
	if err != nil {
		log.Fatal("Failed to put 'age':", err)
	}
	fmt.Println("✓ Stored: age = 30")

	err = node.ClientPut("city", "New York")
	if err != nil {
		log.Fatal("Failed to put 'city':", err)
	}
	fmt.Println("✓ Stored: city = New York")

	// Check log length
	fmt.Printf("\nLog length: %d entries\n", node.GetLogLength())

	// Commit the entries (in a real system, this would happen through replication)
	fmt.Println("\nCommitting entries...")
	node.UpdateCommitIndex(3)
	fmt.Printf("Commit index: 3\n")

	// Retrieve the values
	fmt.Println("\nRetrieving values...")

	value, exists, err := node.ClientGet("name")
	if err != nil {
		log.Fatal("Failed to get 'name':", err)
	}
	if exists {
		fmt.Printf("✓ Retrieved: name = %s\n", value)
	} else {
		fmt.Println("✗ 'name' not found")
	}

	value, exists, err = node.ClientGet("age")
	if err != nil {
		log.Fatal("Failed to get 'age':", err)
	}
	if exists {
		fmt.Printf("✓ Retrieved: age = %s\n", value)
	} else {
		fmt.Println("✗ 'age' not found")
	}

	value, exists, err = node.ClientGet("city")
	if err != nil {
		log.Fatal("Failed to get 'city':", err)
	}
	if exists {
		fmt.Printf("✓ Retrieved: city = %s\n", value)
	} else {
		fmt.Println("✗ 'city' not found")
	}

	// Try to get a non-existent key
	value, exists, err = node.ClientGet("nonexistent")
	if err != nil {
		log.Fatal("Failed to get 'nonexistent':", err)
	}
	if exists {
		fmt.Printf("✓ Retrieved: nonexistent = %s\n", value)
	} else {
		fmt.Println("✓ 'nonexistent' not found (as expected)")
	}

	fmt.Println("\nDemo completed successfully!")

	fmt.Println("\n" + strings.Repeat("=", 50))

	// Run the election demo
	electionDemo()
}
