package turbine

import "errors"

var (
	ErrEmptyEntryBatch               = errors.New("entry batch cannot be empty")
	ErrInvalidBlockComponent         = errors.New("invalid block component")
	ErrUnknownMarkerKind             = errors.New("unknown block marker kind")
	ErrMissingParentMarker           = errors.New("missing parent marker")
	ErrMissingBlockFooter            = errors.New("missing block footer")
	ErrEntryBatchAfterBlockFooter    = errors.New("entry batch after block footer")
	ErrInvalidAlpentickPosition      = errors.New("Alpentick must be the final component after the footer")
	ErrMultipleBlockFooters          = errors.New("multiple block footers")
	ErrMultipleBlockHeaders          = errors.New("multiple block headers")
	ErrMultipleUpdateParents         = errors.New("multiple update parents")
	ErrSpuriousUpdateParent          = errors.New("spurious update parent")
	ErrUnexpectedInitialUpdateParent = errors.New("unexpected initial update parent")
	ErrUpdateParentNotFirstInWindow  = errors.New("update parent outside first slot in leader window")
	ErrGenesisCertificateOutOfOrder  = errors.New("genesis certificate marker must immediately follow block header")
	ErrHeaderParentSlotMismatch      = errors.New("block header parent slot mismatch")
)
