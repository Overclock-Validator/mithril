package turbine

import "fmt"

const alpenglowLeaderWindowSlots = uint64(4)

type componentStreamStage uint8

const (
	componentStagePreParent componentStreamStage = iota
	componentStageAcceptGenesisOrEntries
	componentStageAcceptEntriesOrFooter
	componentStageAcceptAlpentick
	componentStageDone
)

type decodedSlotComponent struct {
	component  BlockComponent
	batchStart uint32
}

// componentStreamProcessor validates the same component ordering as Agave's
// BlockComponentProcessor while extracting the final replay entry section.
type componentStreamProcessor struct {
	slot            uint64
	shredParentSlot uint64
	stage           componentStreamStage
	sawMarker       bool
	legacyEntries   bool
	sawHeader       bool
	sawUpdateParent bool
	sawGenesis      bool
	parent          *AlpenglowParentInfo
	footer          *BlockFooter
	entries         []Entry
}

func (p *componentStreamProcessor) consume(decoded decodedSlotComponent, final bool) error {
	component := decoded.component
	if component.IsEntryBatch() {
		return p.consumeEntries(component.EntryBatch, final)
	}
	if component.IsEmptyEntryBatchAbort() {
		if p.sawMarker {
			return fmt.Errorf("%w: empty entry batch in Alpenglow component stream", ErrInvalidBlockComponent)
		}
		return nil
	}
	if !component.IsMarker() {
		return ErrInvalidBlockComponent
	}
	if p.legacyEntries {
		return fmt.Errorf("%w: block marker after legacy entry batches", ErrInvalidBlockComponent)
	}
	p.sawMarker = true
	marker := component.Marker
	switch marker.Kind {
	case MarkerBlockHeader:
		if marker.Header == nil {
			return ErrInvalidBlockComponent
		}
		if p.sawHeader {
			return ErrMultipleBlockHeaders
		}
		if p.stage != componentStagePreParent || decoded.batchStart != 0 {
			return fmt.Errorf("%w: header at shred %d", ErrInvalidBlockComponent, decoded.batchStart)
		}
		if p.shredParentSlot != 0 && marker.Header.ParentSlot != p.shredParentSlot {
			return fmt.Errorf("%w: header parent %d, shred parent %d", ErrHeaderParentSlotMismatch, marker.Header.ParentSlot, p.shredParentSlot)
		}
		p.sawHeader = true
		p.parent = &AlpenglowParentInfo{
			ParentSlot:        marker.Header.ParentSlot,
			ParentBlockID:     marker.Header.ParentBlockID,
			ReplayFECSetIndex: decoded.batchStart,
		}
		p.stage = componentStageAcceptGenesisOrEntries
		return nil
	case MarkerGenesisCertificate:
		if marker.GenesisCert == nil {
			return ErrInvalidBlockComponent
		}
		if p.stage != componentStageAcceptGenesisOrEntries || p.sawGenesis {
			return ErrGenesisCertificateOutOfOrder
		}
		p.sawGenesis = true
		p.stage = componentStageAcceptEntriesOrFooter
		return nil
	case MarkerUpdateParent:
		if marker.UpdateParent == nil {
			return ErrInvalidBlockComponent
		}
		if p.slot%alpenglowLeaderWindowSlots != 0 {
			return fmt.Errorf("%w: slot %d", ErrUpdateParentNotFirstInWindow, p.slot)
		}
		if p.stage == componentStagePreParent {
			return ErrUnexpectedInitialUpdateParent
		}
		if p.stage == componentStageAcceptAlpentick || p.stage == componentStageDone {
			return ErrSpuriousUpdateParent
		}
		if p.sawUpdateParent {
			return ErrMultipleUpdateParents
		}
		if decoded.batchStart == 0 || decoded.batchStart%dataShredsPerFECBlock != 0 {
			return fmt.Errorf("%w: marker begins at shred %d", ErrInvalidBlockComponent, decoded.batchStart)
		}
		p.sawUpdateParent = true
		p.parent = &AlpenglowParentInfo{
			ParentSlot:        marker.UpdateParent.NewParentSlot,
			ParentBlockID:     marker.UpdateParent.NewParentBlockID,
			ReplayFECSetIndex: decoded.batchStart,
			FromUpdateParent:  true,
		}
		// Agave abandons the bank created from the header and replays only the
		// entry section following UpdateParent.
		p.entries = nil
		p.stage = componentStageAcceptEntriesOrFooter
		return nil
	case MarkerBlockFooter:
		if marker.Footer == nil {
			return ErrInvalidBlockComponent
		}
		if p.stage == componentStagePreParent {
			return ErrMissingParentMarker
		}
		if p.footer != nil || p.stage == componentStageAcceptAlpentick || p.stage == componentStageDone {
			return ErrMultipleBlockFooters
		}
		footer := *marker.Footer
		p.footer = &footer
		p.stage = componentStageAcceptAlpentick
		return nil
	default:
		return ErrUnknownMarkerKind
	}
}

func (p *componentStreamProcessor) consumeEntries(entries []Entry, final bool) error {
	if !p.sawMarker {
		p.legacyEntries = true
		p.entries = append(p.entries, entries...)
		return nil
	}
	switch p.stage {
	case componentStagePreParent:
		return ErrMissingParentMarker
	case componentStageAcceptGenesisOrEntries:
		p.stage = componentStageAcceptEntriesOrFooter
		p.entries = append(p.entries, entries...)
		return nil
	case componentStageAcceptEntriesOrFooter:
		p.entries = append(p.entries, entries...)
		return nil
	case componentStageAcceptAlpentick:
		if !final || !isAlpentick(entries) {
			return ErrEntryBatchAfterBlockFooter
		}
		p.entries = append(p.entries, entries...)
		p.stage = componentStageDone
		return nil
	case componentStageDone:
		return ErrInvalidAlpentickPosition
	default:
		return ErrInvalidBlockComponent
	}
}

func (p *componentStreamProcessor) finish() error {
	if !p.sawMarker {
		return nil
	}
	if p.parent == nil {
		return ErrMissingParentMarker
	}
	if p.footer == nil {
		return ErrMissingBlockFooter
	}
	if p.stage != componentStageDone {
		return ErrInvalidAlpentickPosition
	}
	return nil
}

func isAlpentick(entries []Entry) bool {
	return len(entries) == 1 && entries[0].NumHashes == 1 && len(entries[0].Txns) == 0
}
