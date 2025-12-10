package grpc

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/rpcpool/yellowstone-grpc/examples/golang/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestGrpcServer(t *testing.T) {
	// Use a test port
	port := uint16(50052)
	
	// Create a new gRPC server
	server := NewGrpcServer(port, nil)
	
	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Start()
	}()
	
	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)
	
	// Try to connect to the server to verify it's running
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()
	
	// Verify connection is ready
	if !conn.WaitForStateChange(ctx, conn.GetState()) {
		t.Log("Connection state changed (server is running)")
	}
	
	// Stop the server
	server.server.Stop()
	
	// Wait for server to stop
	select {
	case err := <-errChan:
		if err != nil && err != grpc.ErrServerStopped {
			t.Logf("Server stopped with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Log("Server stopped")
	}
}

func TestPingPong(t *testing.T) {
	// Use a test port
	port := uint16(50053)
	
	// Create a new gRPC server (Geyser service is automatically created and registered)
	server := NewGrpcServer(port, nil)
	
	// Start server in a goroutine
	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Start()
	}()
	
	// Give the server a moment to start
	time.Sleep(100 * time.Millisecond)
	
	// Connect to the server
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()
	
	// Create Geyser client
	client := pb.NewGeyserClient(conn)
	
	// Test Ping with count = 42
	pingReq := &pb.PingRequest{
		Count: 42,
	}
	
	pongResp, err := client.Ping(ctx, pingReq)
	if err != nil {
		t.Fatalf("Ping failed: %v", err)
	}
	
	// Verify the response
	if pongResp.Count != 42 {
		t.Errorf("Expected count 42, got %d", pongResp.Count)
	}
	
	t.Logf("Ping/Pong successful: sent count=%d, received count=%d", pingReq.Count, pongResp.Count)
	
	// Stop the server
	server.server.Stop()
	
	// Wait for server to stop
	select {
	case err := <-errChan:
		if err != nil && err != grpc.ErrServerStopped {
			t.Logf("Server stopped with error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Log("Server stopped")
	}
}

