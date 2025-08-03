package main

import (
	"fmt"
	"raftcluster-kv/raft"
)

func electionDemo() {
	fmt.Println("Raft Leader Election Demo")
	fmt.Println("=========================")

	// Create three nodes to simulate a cluster
	node1 := raft.NewRaftNode("node1", []string{"node2", "node3"})
	node2 := raft.NewRaftNode("node2", []string{"node1", "node3"})
	node3 := raft.NewRaftNode("node3", []string{"node1", "node2"})

	fmt.Printf("Created 3-node cluster: %s, %s, %s\n", "node1", "node2", "node3")
	fmt.Printf("Initial states: node1=%v, node2=%v, node3=%v\n",
		node1.GetState(), node2.GetState(), node3.GetState())

	// Node 1 starts an election
	fmt.Println("\n--- Node 1 starts election ---")
	newTerm := node1.StartElection()
	fmt.Printf("Node 1 started election for term %d\n", newTerm)
	fmt.Printf("Node 1 state: %v\n", node1.GetState())

	// Node 1 sends vote requests to other nodes
	fmt.Println("\n--- Vote requests ---")

	// Node 1 -> Node 2
	request1 := node1.SendRequestVote("node2")
	response1 := node2.HandleRequestVote(request1)
	fmt.Printf("Node 2 response: Term=%d, VoteGranted=%t\n", response1.Term, response1.VoteGranted)

	// Node 1 -> Node 3
	request2 := node1.SendRequestVote("node3")
	response2 := node3.HandleRequestVote(request2)
	fmt.Printf("Node 3 response: Term=%d, VoteGranted=%t\n", response2.Term, response2.VoteGranted)

	// Node 1 processes the responses
	fmt.Println("\n--- Processing responses ---")
	shouldBeLeader1 := node1.ProcessVoteResponse(response1)
	fmt.Printf("After Node 2 response: should be leader = %t\n", shouldBeLeader1)

	shouldBeLeader2 := node1.ProcessVoteResponse(response2)
	fmt.Printf("After Node 3 response: should be leader = %t\n", shouldBeLeader2)

	fmt.Printf("Final Node 1 state: %v\n", node1.GetState())

	// Show what happens when a higher term is seen
	fmt.Println("\n--- Higher term scenario ---")

	// Node 2 starts its own election with higher term
	node2.UpdateTerm(5)
	newTerm2 := node2.StartElection()
	fmt.Printf("Node 2 started election for term %d\n", newTerm2)

	// Node 2 sends request to Node 1
	request3 := node2.SendRequestVote("node1")
	response3 := node1.HandleRequestVote(request3)
	fmt.Printf("Node 1 response to Node 2: Term=%d, VoteGranted=%t\n", response3.Term, response3.VoteGranted)

	// Node 1 should have become follower
	fmt.Printf("Node 1 state after seeing higher term: %v\n", node1.GetState())
	fmt.Printf("Node 1 term after seeing higher term: %d\n", node1.GetCurrentTerm())

	fmt.Println("\nElection demo completed!")
}
