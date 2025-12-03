package grpc

import (
	"context"
	"slices"

	"github.com/Overclock-Validator/mithril/pkg/grpc/pb"
	"google.golang.org/grpc"

	b "github.com/Overclock-Validator/mithril/pkg/block"
	"github.com/gagliardetto/solana-go"
)

type GeyserService struct {
	pb.UnimplementedGeyserServer

	// Channel for sending Blocks to client 
	blockChan chan *b.Block
}


func NewGeyserService() *GeyserService {
	return &GeyserService{
		blockChan: make(chan *b.Block, 100),
	}
}

// GetBlockChannel returns the channel where Block messages can be sent
// External code can send blocks to this channel, and they will be filtered and forwarded to clients
func (s *GeyserService) GetBlockChannel() chan<- *b.Block {
	return s.blockChan
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
	
		// Main loop: process requests and filter updates
	var activeRequest *pb.SubscribeRequest
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
	
	// Goroutine to send blocks to the client
	go func() {
		for {
			select {
			case req, ok := <-s.blockChan:
				if !ok {
					return
				}
				block := s.convertBlockToSubscribeUpdate(req)
				if err := s.sendUpdate(block, activeRequest, updateChan, ctx); err != nil {
					done <- err
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()



	
 for {
		select {
		case req, ok := <-requestChan:
			if !ok {
				return nil
			}
			
			// Handle ping/pong
			if req.Ping != nil {
				pong := &pb.SubscribeUpdate{
					UpdateOneof: &pb.SubscribeUpdate_Pong{
						Pong: &pb.SubscribeUpdatePong{
							Id: req.Ping.Id,
						},
					},
				}
				if err := s.sendUpdate(pong, activeRequest, updateChan, ctx); err != nil {
					return err
				}
				continue
			}
			
			activeRequest = req
			
			
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

// sendUpdate sends an update to the client channel with proper error handling
func (s *GeyserService) sendUpdate(update *pb.SubscribeUpdate, req *pb.SubscribeRequest, updateChan chan<- *pb.SubscribeUpdate, ctx context.Context) error {
	if req != nil {
		update.Filters = s.extractFilterIDs(req)
	}
	select {
	case updateChan <- update:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// shouldSendBlock checks if a Block should be sent based on the active request filters
// This filters the block BEFORE converting to SubscribeUpdate for better performance
func (s *GeyserService) shouldSendBlock(block *b.Block, req *pb.SubscribeRequest) bool {
	if req == nil {
		return true
	}
	
	if len(req.Blocks) == 0 {
		return false
	}
	
	return s.matchesBlockFilterForBlock(block, req)
}

// matchesBlockFilterForBlock checks if a Block matches the request filters
func (s *GeyserService) matchesBlockFilterForBlock(block *b.Block, req *pb.SubscribeRequest) bool {
	for _, filter := range req.Blocks {
		if len(filter.AccountInclude) > 0 {
			// Check if any account in block.UpdatedAccts matches
			for _, pubkey := range block.UpdatedAccts {
				pubkeyStr := pubkey.String()
				if slices.Contains(filter.AccountInclude, pubkeyStr) {
					return true
				}
			}
			
			// Check EpochUpdatedAccts if available
			for _, account := range block.EpochUpdatedAccts {
				if account != nil {
					pubkeyStr := account.Key.String()
					if slices.Contains(filter.AccountInclude, pubkeyStr) {
						return true
					}
				}
			}
			
			// Check ParentEpochUpdatedAccts if available
			for _, account := range block.ParentEpochUpdatedAccts {
				if account != nil {
					pubkeyStr := account.Key.String()
					if slices.Contains(filter.AccountInclude, pubkeyStr) {
						return true
					}
				}
			}

			// If account_include filter exists but no match found, don't send
			return false
		}
		
		// If no specific filters, match all blocks
		return true
	}

	return true
}

// convertBlockToSubscribeUpdate converts a Block to SubscribeUpdate with Block update
func (s *GeyserService) convertBlockToSubscribeUpdate(block *b.Block) *pb.SubscribeUpdate {
	blockUpdate := &pb.SubscribeUpdateBlock{
		Slot:                     block.Slot,
		Blockhash:                solana.Hash(block.Blockhash).String(),
		ParentSlot:               block.ParentSlot,
		ParentBlockhash:          solana.Hash(block.LastBlockhash).String(),
		ExecutedTransactionCount: uint64(len(block.Transactions)),
		UpdatedAccountCount:      uint64(len(block.UpdatedAccts)),
		EntriesCount:             uint64(len(block.Entries)),
	}

	// Set block height if available
	if block.BlockHeight > 0 {
		blockUpdate.BlockHeight = &pb.BlockHeight{
			BlockHeight: block.BlockHeight,
		}
	}

	// Set block time if available
	if block.UnixTimestamp > 0 {
		blockUpdate.BlockTime = &pb.UnixTimestamp{
			Timestamp: block.UnixTimestamp,
		}
	}

	// Convert accounts if available (from UpdatedAccts or EpochUpdatedAccts)
	// Note: This is a simplified conversion - you may need to fetch full account data
	accounts := make([]*pb.SubscribeUpdateAccountInfo, 0, len(block.UpdatedAccts))
	for _, pubkey := range block.UpdatedAccts {
		accounts = append(accounts, &pb.SubscribeUpdateAccountInfo{
			Pubkey: pubkey[:],
		})
	}
	blockUpdate.Accounts = accounts

	return &pb.SubscribeUpdate{
		UpdateOneof: &pb.SubscribeUpdate_Block{
			Block: blockUpdate,
		},
	}
}
