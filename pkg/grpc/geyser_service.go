package grpc

import (
	"context"
	"slices"

	"github.com/Overclock-Validator/mithril/pkg/grpc/pb"
	"google.golang.org/grpc"
)

type GeyserService struct {
	pb.UnimplementedGeyserServer
	
	// Channel for receiving SubscribeUpdate messages that need to be filtered and sent
	updateChan chan *pb.SubscribeUpdate
}


func NewGeyserService() *GeyserService {
	return &GeyserService{
		updateChan: make(chan *pb.SubscribeUpdate, 100),
	}
}

// GetUpdateChannel returns the channel where SubscribeUpdate messages can be sent
// External code can send messages to this channel, and they will be filtered and forwarded
func (s *GeyserService) GetUpdateChannel() chan<- *pb.SubscribeUpdate {
	return s.updateChan
}

func (s *GeyserService) Ping(ctx context.Context, req *pb.PingRequest) (*pb.PongResponse, error) {
	return &pb.PongResponse{
		Count: req.Count,
	}, nil
}

func (s *GeyserService) Subscribe(stream grpc.BidiStreamingServer[pb.SubscribeRequest, pb.SubscribeUpdate]) error {
	ctx := stream.Context()
	
	// Channel for receiving SubscribeRequest messages from the client
	requestChan := make(chan *pb.SubscribeRequest, 10)
	
	// Channel for sending SubscribeUpdate messages to the client
	updateChan := make(chan *pb.SubscribeUpdate, 100)
	
	// Channel to signal when we're done
	done := make(chan error, 1)
	
	// Goroutine to receive SubscribeRequest messages from the client
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				done <- err
				close(requestChan)
				return
			}
			requestChan <- req
		}
	}()
	
	// Goroutine to send SubscribeUpdate messages to the client
	go func() {
		for {
			select {
			case update := <-updateChan:
				if err := stream.Send(update); err != nil {
					done <- err
					return
				}
			case <-ctx.Done():
				done <- ctx.Err()
				return
			}
		}
	}()
	
	// Main loop: process requests and filter updates
	var activeRequest *pb.SubscribeRequest
	
	for {
		select {
		case req, ok := <-requestChan:
			if !ok {
				// Client closed the request stream
				return nil
			}
			
			// Handle ping/pong
			if req.Ping != nil {
				filterIDs := s.extractFilterIDs(activeRequest)
				pong := &pb.SubscribeUpdate{
					Filters: filterIDs,
					UpdateOneof: &pb.SubscribeUpdate_Pong{
						Pong: &pb.SubscribeUpdatePong{
							Id: req.Ping.Id,
						},
					},
				}
				select {
				case updateChan <- pong:
				case <-ctx.Done():
					return ctx.Err()
				}
				continue
			}
			
			activeRequest = req
			
		case update := <-s.updateChan:
			if s.shouldSendUpdate(update, activeRequest) {
				if activeRequest != nil {
					update.Filters = s.extractFilterIDs(activeRequest)
				}
				select {
				case updateChan <- update:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			
		case err := <-done:
			return err
			
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *GeyserService) extractFilterIDs(req *pb.SubscribeRequest) []string {
	var filterIDs []string
	
	if req != nil && len(req.Blocks) > 0 {
		for id := range req.Blocks {
			filterIDs = append(filterIDs, id)
		}
	}
	
	return filterIDs
}

func (s *GeyserService) shouldSendUpdate(update *pb.SubscribeUpdate, req *pb.SubscribeRequest) bool {
	if req == nil {
		return true
	}
	
	// Always send ping/pong messages
	switch update.UpdateOneof.(type) {
	case *pb.SubscribeUpdate_Ping, *pb.SubscribeUpdate_Pong:
		return true
	case *pb.SubscribeUpdate_Block:
		return s.matchesBlockFilter(update.GetBlock(), req)
	default:
		return false
	}
}

func (s *GeyserService) matchesBlockFilter(block *pb.SubscribeUpdateBlock, req *pb.SubscribeRequest) bool {
	if len(req.Blocks) == 0 {
		return false
	}
	
	for _, filter := range req.Blocks {
		if len(filter.AccountInclude) > 0 {
			accounts := block.GetAccounts()

			for _, account := range accounts {
				if slices.Contains(filter.AccountInclude, string(account.Pubkey)) {
					return true
				}
			}

			// TODO build other filters here
			return false
		}
	}

	return true
}
