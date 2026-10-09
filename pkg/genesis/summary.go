package genesis

import "encoding/json"

func marshalSummary(g *Genesis, c Config, m BankMetadata) ([]byte, error) {
	return json.MarshalIndent(struct {
		Profile       string       `json:"profile"`
		AgaveRevision string       `json:"agave_revision"`
		GenesisHash   string       `json:"genesis_hash"`
		Config        Config       `json:"resolved_config"`
		InitialBank   BankMetadata `json:"completed_initial_bank"`
	}{Profile, AgaveRevision, m.GenesisHash, c, m}, "", "  ")
}
