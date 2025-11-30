package grpc

import (
	"fmt"
	"log"
	"net"

	"github.com/Overclock-Validator/mithril/pkg/grpc/pb"
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

