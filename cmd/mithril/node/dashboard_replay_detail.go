package node

import (
	"errors"
	"time"

	"github.com/Overclock-Validator/mithril/pkg/dashboard"
	"github.com/Overclock-Validator/mithril/pkg/metrics"
	"github.com/Overclock-Validator/mithril/pkg/replay"
)

// replayDetailCore maps a detached source copy, never live row metrics. The
// caller must fence this copy against current progress before publication.
func replayDetailCore(s replay.ReplayDiagnosticSnapshot, nodeStartedAt, capturedAt time.Time, genesisHash *string) (dashboard.ReplayDetailCore, error) {
	if len(s.Records) > dashboard.ReplayDetailCapacity {
		return dashboard.ReplayDetailCore{}, errors.New("replay detail source exceeds capacity")
	}
	c := dashboard.ReplayDetailCore{
		SchemaVersion: 2, Family: "replay", Scope: "replay_row_history",
		Binding: dashboard.ReplayDetailBinding{
			RunID: s.RunID, NodeStartedAt: nodeStartedAt.UTC(), SourceGeneration: s.Generation,
			Attempt: s.Attempt, Rewinds: s.Rewinds, AttemptStartedAt: s.StartedAt.UTC(), ResetAt: s.ResetAt.UTC(),
		},
		CapturedAt: capturedAt.UTC(), UpdatedAt: s.UpdatedAt.UTC(),
		Capacity: dashboard.ReplayDetailCapacity, Count: len(s.Records),
		FirstSequence: s.FirstSequence, LastSequence: s.LastSequence, Evicted: s.Evicted, Exhausted: s.Exhausted,
		Records: make([]dashboard.ReplayDetailRecord, len(s.Records)),
	}
	if genesisHash != nil {
		value := *genesisHash
		c.Binding.GenesisHash = &value
	}
	if o := s.NearReception; !o.ObservedAt.IsZero() {
		c.NearReception = &dashboard.ReplayNearReception{Scope: "first_completed_replay_near_recent_input", Slot: o.Slot, Epoch: o.Epoch,
			Bankhash: o.Bankhash.String(), CompletedAt: o.CompletedAt.UTC(), ObservedAt: o.ObservedAt.UTC(), SourceStartedAt: o.SourceStartedAt.UTC(),
			ReceiverStartedAt: o.ReceiverStartedAt.UTC(), ReceiverSession: o.ReceiverSession, ReceptionSlot: o.ReceptionSlot,
			ReceptionSampledAt: o.ReceptionSampledAt.UTC(), ReceptionObservedAt: o.ReceptionObservedAt.UTC(), RetainedSlots: o.RetainedSlots}
	}
	if o := s.FoldCapacity; o.State != "" {
		c.FoldCapacity = &dashboard.ReplayFoldCapacity{Scope: "replay_fold_capacity", State: o.State, Slot: o.Slot,
			StartedAt: o.StartedAt.UTC(), HadInFlightFold: o.HadInFlightFold, Outcome: o.Outcome}
		if !o.ReturnedAt.IsZero() {
			at := o.ReturnedAt.UTC()
			c.FoldCapacity.ReturnedAt = &at
		}
		if o.DurationKnown {
			n := o.DurationNanoseconds
			c.FoldCapacity.DurationNanoseconds = &n
		}
	}
	for i, r := range s.Records {
		c.Records[i] = dashboard.ReplayDetailRecord{
			Sequence:                          r.Sequence,
			CompletedIndex:                    r.CompletedIndex,
			Attempt:                           r.Attempt,
			Rewinds:                           r.Rewinds,
			Slot:                              r.Slot,
			Epoch:                             r.Epoch,
			BlockHeight:                       r.BlockHeight,
			Transactions:                      r.Transactions,
			ComputeUnits:                      r.ComputeUnits,
			WorkCountersSaturated:             r.WorkCountersSaturated,
			SharedExecutionTimingsUnavailable: r.SharedExecutionTimingsUnavailable,
			DependencyPlannerPreparedObserved: r.DependencyPlannerPreparedObserved,
			DependencyPlannerFallbackObserved: r.DependencyPlannerFallbackObserved,
			UnknownStages:                     r.UnknownStages,
			UnrootedRetainedBytes:             r.UnrootedRetainedBytes,
			UnrootedHeldSlots:                 r.UnrootedHeldSlots,
			RetentionLimits: dashboard.ReplayDetailRetentionLimits{Observed: r.RetentionLimits.Observed,
				RetainedBytesPauseAt: r.RetentionLimits.RetainedBytesPauseAt, HeldSlotsHaltAbove: r.RetentionLimits.HeldSlotsHaltAbove},
			Bankhash: r.Bankhash.String(), CompletedAt: r.CompletedAt.UTC(), ObservedAt: r.ObservedAt.UTC(), Scope: r.Scope.String(),
			Streaming: dashboard.ReplayDetailStreaming{
				OpenedObserved: r.Streaming.OpenedObserved, DiscardObserved: r.Streaming.DiscardObserved,
				RecordedGroups: r.Streaming.RecordedGroups, RecordedTransactions: r.Streaming.RecordedTransactions,
				LastDiscardReason:           r.Streaming.LastDiscardReason.String(),
				AttemptBreakdownUnavailable: r.Streaming.AttemptBreakdownUnavailable,
			},
			Timings: dashboard.ReplayDetailTimings{
				SinceLastRow:                     replayDetailTiming(r.Timings.SinceLastRow),
				LoopIteration:                    replayDetailTiming(r.Timings.LoopIteration),
				LoopSourceWait:                   replayDetailTiming(r.Timings.LoopSourceWait),
				LoopBeforeReplay:                 replayDetailTiming(r.Timings.LoopBeforeReplay),
				LoopAfterReplay:                  replayDetailTiming(r.Timings.LoopAfterReplay),
				SlotReplay:                       replayDetailTiming(r.Timings.SlotReplay),
				PreprocessBlock:                  replayDetailTiming(r.Timings.PreprocessBlock),
				ProcessBlock:                     replayDetailTiming(r.Timings.ProcessBlock),
				PostProcessBlock:                 replayDetailTiming(r.Timings.PostProcessBlock),
				ProcessBlockUnstaged:             replayDetailTiming(r.Timings.ProcessBlockUnstaged),
				RewardCertificatePreflight:       replayDetailTiming(r.Timings.RewardCertificatePreflight),
				TransactionExecutionPlan:         replayDetailTiming(r.Timings.TransactionExecutionPlan),
				TransactionStatusValidation:      replayDetailTiming(r.Timings.TransactionStatusValidation),
				DependencyPlannerPreparation:     replayDetailTiming(r.Timings.DependencyPlannerPreparation),
				DependencyPlannerBuild:           replayDetailTiming(r.Timings.DependencyPlannerBuild),
				DependencyPlannerWait:            replayDetailTiming(r.Timings.DependencyPlannerWait),
				DependencyPlannerDispatch:        replayDetailTiming(r.Timings.DependencyPlannerDispatch),
				LoadBlockAccounts:                replayDetailTiming(r.Timings.LoadBlockAccounts),
				SlotCtxSetup:                     replayDetailTiming(r.Timings.SlotCtxSetup),
				TxLoop:                           replayDetailTiming(r.Timings.TxLoop),
				Reward:                           replayDetailTiming(r.Timings.Reward),
				Rent:                             replayDetailTiming(r.Timings.Rent),
				RunIncinerator:                   replayDetailTiming(r.Timings.RunIncinerator),
				AlpenglowFooterClock:             replayDetailTiming(r.Timings.AlpenglowFooterClock),
				AlpenglowVoteRewards:             replayDetailTiming(r.Timings.AlpenglowVoteRewards),
				AlpenglowFooterVerification:      replayDetailTiming(r.Timings.AlpenglowFooterVerification),
				CompileWritableAndModifiedAccts:  replayDetailTiming(r.Timings.CompileWritableAndModifiedAccts),
				EnsureParentAccountsForModified:  replayDetailTiming(r.Timings.EnsureParentAccountsForModified),
				BlockUpdateAccounts:              replayDetailTiming(r.Timings.BlockUpdateAccounts),
				TransactionStatusCommit:          replayDetailTiming(r.Timings.TransactionStatusCommit),
				TransactionStatusPreparation:     replayDetailTiming(r.Timings.TransactionStatusPreparation),
				TransactionStatusPreparationWait: replayDetailTiming(r.Timings.TransactionStatusPreparationWait),
				SignatureVerificationJoin:        replayDetailTiming(r.Timings.SignatureVerificationJoin),
				AccountsDeltaHash:                replayDetailTiming(r.Timings.AccountsDeltaHash),
				LtHashDedupe:                     replayDetailTiming(r.Timings.LtHashDedupe),
				LtHashWorkerCompute:              replayDetailTiming(r.Timings.LtHashWorkerCompute),
				LtHashPartialReduce:              replayDetailTiming(r.Timings.LtHashPartialReduce),
				BankHashFinalize:                 replayDetailTiming(r.Timings.BankHashFinalize),
				BankHash:                         replayDetailTiming(r.Timings.BankHash),
				TransactionStatusView:            replayDetailTiming(r.Timings.TransactionStatusView),
				ChainTipUpdate:                   replayDetailTiming(r.Timings.ChainTipUpdate),
				ResumeContext:                    replayDetailTiming(r.Timings.ResumeContext),
				FullToReplayed:                   replayDetailTiming(r.Timings.FullToReplayed),
				FoldOverlap:                      replayDetailTiming(r.Timings.FoldOverlap),
			},
			Work: dashboard.ReplayDetailWork{
				Sigverify:              replayDetailTiming(r.Work.Sigverify),
				TxUpdateAccounts:       replayDetailTiming(r.Work.TxUpdateAccounts),
				TxFailedUpdateAccounts: replayDetailTiming(r.Work.TxFailedUpdateAccounts),
			},
			Stages: dashboard.ReplayDetailStages{
				ParameterArena:         replayDetailTiming(r.Stages[replay.ReplayStageParameterArena]),
				DependencyPlanner:      replayDetailTiming(r.Stages[replay.ReplayStageDependencyPlanner]),
				LoadAccounts:           replayDetailTiming(r.Stages[replay.ReplayStageLoadAccounts]),
				TransactionLoop:        replayDetailTiming(r.Stages[replay.ReplayStageTransactionLoop]),
				DistributeFees:         replayDetailTiming(r.Stages[replay.ReplayStageDistributeFees]),
				CollectRent:            replayDetailTiming(r.Stages[replay.ReplayStageCollectRent]),
				Incinerator:            replayDetailTiming(r.Stages[replay.ReplayStageIncinerator]),
				CompileAccounts:        replayDetailTiming(r.Stages[replay.ReplayStageCompileAccounts]),
				BankHash:               replayDetailTiming(r.Stages[replay.ReplayStageBankHash]),
				StoreAccounts:          replayDetailTiming(r.Stages[replay.ReplayStageStoreAccounts]),
				Done:                   replayDetailTiming(r.Stages[replay.ReplayStageDone]),
				StreamingWait:          replayDetailTiming(r.Stages[replay.ReplayStageStreamingWait]),
				StreamingSignatureWait: replayDetailTiming(r.Stages[replay.ReplayStageStreamingSignatureWait]),
			},
			AccountLoader: dashboard.ReplayDetailAccountLoader{
				AddressTableLookups:                replayDetailTiming(r.AccountLoader.AddressTableLookups),
				DedupeBlockAccounts:                replayDetailTiming(r.AccountLoader.DedupeBlockAccounts),
				SourceBatch:                        replayDetailTiming(r.AccountLoader.SourceBatch),
				ParentMapBuild:                     replayDetailTiming(r.AccountLoader.ParentMapBuild),
				SysvarUpdates:                      replayDetailTiming(r.AccountLoader.SysvarUpdates),
				SysvarClockRead:                    replayDetailTiming(r.AccountLoader.SysvarClockRead),
				SysvarSlotHashesRead:               replayDetailTiming(r.AccountLoader.SysvarSlotHashesRead),
				SysvarRecentBlockhashesRead:        replayDetailTiming(r.AccountLoader.SysvarRecentBlockhashesRead),
				SysvarSlotHistoryRead:              replayDetailTiming(r.AccountLoader.SysvarSlotHistoryRead),
				SysvarStakeHistoryRead:             replayDetailTiming(r.AccountLoader.SysvarStakeHistoryRead),
				SysvarLastRestartSlotRead:          replayDetailTiming(r.AccountLoader.SysvarLastRestartSlotRead),
				WorkingSetLookup:                   replayDetailTiming(r.AccountLoader.WorkingSetLookup),
				AppendVecPinWait:                   replayDetailTiming(r.AccountLoader.AppendVecPinWait),
				ReadCacheEpochWait:                 replayDetailTiming(r.AccountLoader.ReadCacheEpochWait),
				CacheLookup:                        replayDetailTiming(r.AccountLoader.CacheLookup),
				AdmissionFilter:                    replayDetailTiming(r.AccountLoader.AdmissionFilter),
				IndexLookup:                        replayDetailTiming(r.AccountLoader.IndexLookup),
				ReadPlanning:                       replayDetailTiming(r.AccountLoader.ReadPlanning),
				AppendVecRead:                      replayDetailTiming(r.AccountLoader.AppendVecRead),
				CachePublicationWait:               replayDetailTiming(r.AccountLoader.CachePublicationWait),
				CachePublication:                   replayDetailTiming(r.AccountLoader.CachePublication),
				SysvarWorkingSetLookup:             replayDetailTiming(r.AccountLoader.SysvarWorkingSetLookup),
				SysvarClone:                        replayDetailTiming(r.AccountLoader.SysvarClone),
				SysvarAppendVecPinWait:             replayDetailTiming(r.AccountLoader.SysvarAppendVecPinWait),
				SysvarReadCacheEpochWait:           replayDetailTiming(r.AccountLoader.SysvarReadCacheEpochWait),
				SysvarCacheLookup:                  replayDetailTiming(r.AccountLoader.SysvarCacheLookup),
				SysvarIndexAndAppendVecRead:        replayDetailTiming(r.AccountLoader.SysvarIndexAndAppendVecRead),
				SysvarCachePublicationWait:         replayDetailTiming(r.AccountLoader.SysvarCachePublicationWait),
				SysvarCachePublication:             replayDetailTiming(r.AccountLoader.SysvarCachePublication),
				RequestedKeys:                      r.AccountLoader.RequestedKeys,
				DurableKeys:                        r.AccountLoader.DurableKeys,
				ParentAccounts:                     r.AccountLoader.ParentAccounts,
				WorkingSetHits:                     r.AccountLoader.WorkingSetHits,
				PendingFoldHits:                    r.AccountLoader.PendingFoldHits,
				CacheHits:                          r.AccountLoader.CacheHits,
				NegativeCacheHits:                  r.AccountLoader.NegativeCacheHits,
				IndexHits:                          r.AccountLoader.IndexHits,
				IndexMisses:                        r.AccountLoader.IndexMisses,
				UniqueAppendVecs:                   r.AccountLoader.UniqueAppendVecs,
				AppendVecChunks:                    r.AccountLoader.AppendVecChunks,
				AppendVecAccounts:                  r.AccountLoader.AppendVecAccounts,
				OpenFailures:                       r.AccountLoader.OpenFailures,
				ReadFailures:                       r.AccountLoader.ReadFailures,
				RetryAccounts:                      r.AccountLoader.RetryAccounts,
				CommonCacheAdmissions:              r.AccountLoader.CommonCacheAdmissions,
				CommonCacheAdmissionsSkipped:       r.AccountLoader.CommonCacheAdmissionsSkipped,
				VoteCacheAdmissions:                r.AccountLoader.VoteCacheAdmissions,
				VoteCacheAdmissionsSkipped:         r.AccountLoader.VoteCacheAdmissionsSkipped,
				CachePublicationEpochRejects:       r.AccountLoader.CachePublicationEpochRejects,
				DecodedAccountObjects:              r.AccountLoader.DecodedAccountObjects,
				DecodedAccountBytes:                r.AccountLoader.DecodedAccountBytes,
				PlaceholderObjects:                 r.AccountLoader.PlaceholderObjects,
				SysvarReads:                        r.AccountLoader.SysvarReads,
				SysvarWorkingSetHits:               r.AccountLoader.SysvarWorkingSetHits,
				SysvarPendingFoldHits:              r.AccountLoader.SysvarPendingFoldHits,
				SysvarCacheHits:                    r.AccountLoader.SysvarCacheHits,
				SysvarDurableReads:                 r.AccountLoader.SysvarDurableReads,
				SysvarCachePublicationEpochRejects: r.AccountLoader.SysvarCachePublicationEpochRejects,
			},
		}
	}
	if err := dashboard.ValidateReplayDetailCore(c); err != nil {
		return dashboard.ReplayDetailCore{}, err
	}
	return c, nil
}

func replayDetailTiming(t metrics.Timing) dashboard.ReplayDetailTiming {
	return dashboard.ReplayDetailTiming{Count: t.Count, SumNanoseconds: t.SumNanoseconds}
}
