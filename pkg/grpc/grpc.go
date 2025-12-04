package grpc

import (
	"fmt"
	"log"
	"net"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	pb "github.com/rpcpool/yellowstone-grpc/examples/golang/proto"
	"google.golang.org/grpc"
)

type GrpcServer struct {
	server        *grpc.Server
	listener      net.Listener
	port          uint16
	geyserService *GeyserService
}

func NewGrpcServer(port uint16, opts []grpc.ServerOption) *GrpcServer {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	server := grpc.NewServer(opts...)

	return &GrpcServer{
		port:          port,
		listener:      lis,
		server:        server,
		geyserService: NewGeyserService(),
	}
}

func (s *GrpcServer) Start() error {
	pb.RegisterGeyserServer(s.server, s.geyserService)
	return s.server.Serve(s.listener)
}

func (s *GrpcServer) GracefulStop() {
	s.server.GracefulStop()
}

// GetBlockChannel returns the channel where Block messages can be sent.
// External packages can send blocks to this channel, and they will be filtered and forwarded to clients.
//
// Example usage from another package:
//
//	server := grpc.NewGrpcServer(50051, nil)
//	blockChan := server.GetBlockChannel()
//	
//	// Send a block (will be converted to SubscribeUpdate and filtered)
//	block := &block.Block{
//		Slot: 12345,
//		Blockhash: [32]byte{...},
//		// ... other fields
//	}
//	blockChan <- block
func (s *GrpcServer) GetBlockChannel() chan<- *b.Block {
	return s.geyserService.GetBlockChannel()
}

