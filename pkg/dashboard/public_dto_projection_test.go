package dashboard

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestPublicSnapshotUsesPublicNestedTypes(t *testing.T) {
	seen := make(map[reflect.Type]bool)
	var walk func(reflect.Type)
	walk = func(typ reflect.Type) {
		for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array || typ.Kind() == reflect.Map {
			typ = typ.Elem()
		}
		if typ.Kind() != reflect.Struct || typ.PkgPath() != reflect.TypeOf(PublicSnapshot{}).PkgPath() || seen[typ] {
			return
		}
		seen[typ] = true
		if !strings.HasPrefix(typ.Name(), "Public") && typ != reflect.TypeOf(VoteTicketCapacity{}) {
			t.Errorf("private type %s is used in the public snapshot", typ.Name())
		}
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.IsExported() && field.Tag.Get("json") != "-" {
				walk(field.Type)
			}
		}
	}
	walk(reflect.TypeOf(PublicSnapshot{}))
}

func TestPublicNestedDTOsHaveIndependentTypesAndWireShapes(t *testing.T) {
	tests := []struct {
		name    string
		private any
		public  any
		want    string
	}{
		{"receiver", Receiver{}, PublicReceiver{DatagramsReceived: 1}, `{"datagrams_received":"1","messages_decoded":"0","decode_errors":"0","read_errors":"0","connections_rejected":"0","rate_limited_datagrams":"0","last_message_at":null}`},
		{"admission", TurbineAdmission{}, PublicTurbineAdmission{MissingLeaders: 1, ShredVersionMismatch: 2}, `{"missing_leaders":"1","shred_version_mismatch":"2"}`},
		{"epoch", EpochOutcome{}, PublicEpochOutcome{Epoch: 1, FirstSlot: 2, LastSlot: 3}, `{"epoch":"1","first_slot":"2","last_slot":"3","leaders":null,"rewards":null}`},
		{"leader window", EpochLeaderWindow{}, PublicEpochLeaderWindow{Produced: 1}, `{"first_slot":"0","last_slot":"0","produced":"1","selected_fork_skipped":"0","scheduled_unknown":"0","unknown_schedule_slots":"0","unknown_ancestry_slots":"0","complete_epoch":false}`},
		{"reward window", EpochRewardWindow{}, PublicEpochRewardWindow{Included: 1}, `{"first_slot":"0","last_slot":"0","included":"1","excluded":"0","ineligible":"0","unknown":"0","unobserved":"0","complete_epoch":false}`},
		{"reward target", RewardTarget{}, PublicRewardTarget{Slot: 1, Epoch: 2, Outcome: "included"}, `{"target_slot":"1","target_epoch":"2","outcome":"included"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if reflect.TypeOf(tt.private) == reflect.TypeOf(tt.public) {
				t.Fatal("public and private payloads must use independent types")
			}
			got, err := json.Marshal(tt.public)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Fatalf("wire shape: got %s, want %s", got, tt.want)
			}
		})
	}
	for _, tt := range []struct {
		parent any
		field  string
		want   reflect.Type
	}{
		{PublicConsensus{}, "Receiver", reflect.TypeOf((*PublicReceiver)(nil))},
		{PublicTurbineDiagnostics{}, "Admission", reflect.TypeOf((*PublicTurbineAdmission)(nil))},
		{PublicEpochOutcomes{}, "Epochs", reflect.TypeOf([]PublicEpochOutcome(nil))},
		{PublicEpochOutcome{}, "Leaders", reflect.TypeOf((*PublicEpochLeaderWindow)(nil))},
		{PublicEpochOutcome{}, "Rewards", reflect.TypeOf((*PublicEpochRewardWindow)(nil))},
		{PublicRewardHealth{}, "LatestResolved", reflect.TypeOf((*PublicRewardTarget)(nil))},
		{PublicRewardHealth{}, "LatestIncluded", reflect.TypeOf((*PublicRewardTarget)(nil))},
	} {
		field, ok := reflect.TypeOf(tt.parent).FieldByName(tt.field)
		if !ok || field.Type != tt.want {
			t.Errorf("%T.%s type = %v, want %v", tt.parent, tt.field, field.Type, tt.want)
		}
	}
}
