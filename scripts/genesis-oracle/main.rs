// Run only in an isolated copy of Agave at the revision recorded by run.sh.
use base64::{engine::general_purpose::STANDARD, Engine};
use serde_json::{json, Value};
use solana_account::ReadableAccount;
use solana_genesis_config::GenesisConfig;
use solana_runtime::{bank::Bank, genesis_utils};
use std::{env, fs, sync::Arc};

// Deterministic bank-replay fixtures. These are entry streams and bank states,
// not a signed shred ledger or a running consensus cluster. All keys below
// are public test seeds; production genesis never generates private keys.
fn replay(g: &GenesisConfig, output: &str) {
    use solana_entry::entry::{create_ticks, Entry};
    use solana_keypair::keypair_from_seed;
    use solana_signer::Signer;
    let sender = keypair_from_seed(&[7; 32]).unwrap();
    let receiver = keypair_from_seed(&[8; 32]).unwrap();
    assert!(g.accounts.contains_key(&sender.pubkey()));
    assert!(g.accounts.contains_key(&receiver.pubkey()));
    let (mut parent, bank_forks) = Bank::new_with_bank_forks_for_tests(g);
    for tick in create_ticks(g.ticks_per_slot, 0, g.hash()) {
        parent.register_tick_for_test(&tick.hash);
    }
    parent.freeze();
    let genesis = state(&parent);
    let mut blocks = Vec::new();
    for slot in 1..=4 {
        let leader = solana_runtime::leader_schedule_utils::slot_leader_at(slot, &parent).unwrap();
        let bank = Bank::new_from_parent(Arc::clone(&parent), &leader, slot);
        let bank = bank_forks
            .write()
            .unwrap()
            .insert(bank)
            .clone_without_scheduler();
        let initialized = state(&bank);
        let transactions = if slot % 2 == 0 {
            vec![solana_system_transaction::transfer(
                &sender,
                &receiver.pubkey(),
                slot * 1_000_000,
                parent.last_blockhash(),
            )]
        } else {
            vec![]
        };
        let mut entries = Vec::new();
        let mut last_hash = parent.last_blockhash();
        if !transactions.is_empty() {
            let entry = Entry::new(&last_hash, 1, transactions.clone());
            assert!(entry.verify(&last_hash));
            last_hash = entry.hash;
            entries.push(entry);
        }
        for tx in &transactions {
            tx.verify().unwrap();
            bank.process_transaction(tx).unwrap();
        }
        for tick in create_ticks(g.ticks_per_slot, 0, last_hash) {
            bank.register_tick_for_test(&tick.hash);
            entries.push(tick);
        }
        let nanos = g.creation_time as u64 * 1_000_000_000 + slot * 400_000_000;
        bank.update_clock_from_footer(nanos.try_into().unwrap());
        bank.freeze();
        blocks.push(json!({"slot":slot, "parent_slot":parent.slot(),
            "producer_time_nanos":nanos,
            "entries":STANDARD.encode(bincode::serialize(&entries).unwrap()),
            "transactions":transactions.iter().map(|tx| STANDARD.encode(bincode::serialize(tx).unwrap())).collect::<Vec<_>>(),
            "initialized":initialized, "frozen":state(&bank)}));
        parent = bank;
    }
    fs::write(
        output,
        serde_json::to_vec_pretty(&json!({
            "agave_revision":"7e51da963aee49622a395f562386a6bd8ba0e717",
            "genesis_hash":g.hash().to_string(), "genesis":genesis,"blocks":blocks
        }))
        .unwrap(),
    )
    .unwrap();
}

// Execute the native producer's actual entries. No expected bank hash or
// accounts are supplied as inputs. Complete the single alpentick exactly as
// core/block_creation_loop.rs::record_and_complete_block does in this revision.
fn replay_native(g: &GenesisConfig, input: &str, output: &str) {
    use solana_entry::entry::{create_ticks, Entry};
    let input: Vec<Value> = serde_json::from_slice(&fs::read(input).unwrap()).unwrap();
    let (mut parent, forks) = Bank::new_with_bank_forks_for_tests(g);
    for tick in create_ticks(g.ticks_per_slot, 0, g.hash()) {
        parent.register_tick_for_test(&tick.hash);
    }
    parent.freeze();
    let genesis = state(&parent);
    let mut blocks = Vec::new();
    for request in input {
        let slot = request["slot"].as_u64().unwrap();
        assert_eq!(slot, parent.slot() + 1);
        assert_eq!(request["parent_slot"].as_u64().unwrap(), parent.slot());
        let leader = solana_runtime::leader_schedule_utils::slot_leader_at(slot, &parent).unwrap();
        let bank = Bank::new_from_parent(Arc::clone(&parent), &leader, slot);
        let bank = forks
            .write()
            .unwrap()
            .insert(bank)
            .clone_without_scheduler();
        let initialized = state(&bank);
        let raw = STANDARD
            .decode(request["entries"].as_str().unwrap())
            .unwrap();
        let entries: Vec<Entry> = bincode::deserialize(&raw).unwrap();
        assert_eq!(raw, bincode::serialize(&entries).unwrap());
        assert!(!entries.is_empty());
        let mut last_hash = parent.last_blockhash();
        let mut transactions = Vec::new();
        for (i, entry) in entries.iter().enumerate() {
            assert_eq!(entry.num_hashes, 1);
            assert!(entry.verify(&last_hash));
            last_hash = entry.hash;
            if i + 1 == entries.len() {
                assert!(entry.transactions.is_empty());
                bank.set_tick_height(bank.max_tick_height() - 1);
                bank.register_tick_for_test(&entry.hash);
            } else {
                assert!(!entry.transactions.is_empty());
                for tx in &entry.transactions {
                    let tx = tx
                        .clone()
                        .into_legacy_transaction()
                        .expect("native fixture uses legacy system transfers");
                    tx.verify().unwrap();
                    bank.process_transaction(&tx).unwrap();
                    transactions.push(STANDARD.encode(bincode::serialize(&tx).unwrap()));
                }
            }
        }
        assert!(bank.is_complete());
        let nanos = request["producer_time_nanos"].as_u64().unwrap();
        bank.update_clock_from_footer(nanos.try_into().unwrap());
        bank.freeze();
        blocks.push(json!({"slot":slot,"parent_slot":parent.slot(),"producer_time_nanos":nanos,
            "entries":STANDARD.encode(raw),"transactions":transactions,"initialized":initialized,"frozen":state(&bank)}));
        parent = bank;
    }
    fs::write(
        output,
        serde_json::to_vec_pretty(
            &json!({"agave_revision":"7e51da963aee49622a395f562386a6bd8ba0e717",
        "genesis_hash":g.hash().to_string(),"genesis":genesis,"blocks":blocks}),
        )
        .unwrap(),
    )
    .unwrap();
}

// Independently check account construction, not just how Bank consumes the
// serialized accounts. In particular this checks V4 allocation/padding and
// Agave's fully-active genesis stake semantics.
fn validate_validator_accounts(g: &GenesisConfig) {
    for (key, a) in &g.accounts {
        if a.owner == solana_sdk_ids::vote::id() {
            let v = solana_vote_interface::state::VoteStateV4::deserialize(&a.data, key).unwrap();
            let expected = solana_vote_program::vote_state::create_v4_account_with_authorized(
                &v.node_pubkey,
                &v.authorized_voters.get_authorized_voter(0).unwrap(),
                v.bls_pubkey_compressed.unwrap(),
                &v.authorized_withdrawer,
                v.inflation_rewards_commission_bps,
                &v.inflation_rewards_collector,
                v.block_revenue_commission_bps,
                &v.block_revenue_collector,
                a.lamports,
            );
            assert_eq!(
                a.data,
                expected.data(),
                "vote construction differs from Agave: {key}"
            );
        } else if a.owner == solana_sdk_ids::stake::id() {
            use solana_stake_interface::{
                stake_flags::StakeFlags,
                state::{Delegation, Meta, Stake, StakeStateV2},
            };
            let state: StakeStateV2 = bincode::deserialize(&a.data).unwrap();
            let StakeStateV2::Stake(meta, stake, _) = state else {
                panic!("nondelegated genesis stake")
            };
            let reserve = g.rent.minimum_balance(StakeStateV2::size_of());
            let expected = StakeStateV2::Stake(
                Meta {
                    rent_exempt_reserve: reserve,
                    authorized: meta.authorized,
                    ..Meta::default()
                },
                Stake {
                    delegation: Delegation::new(
                        &stake.delegation.voter_pubkey,
                        a.lamports - reserve,
                        u64::MAX,
                    ),
                    credits_observed: 0,
                },
                StakeFlags::empty(),
            );
            let mut data = bincode::serialize(&expected).unwrap();
            data.resize(StakeStateV2::size_of(), 0);
            assert_eq!(a.data, data, "stake construction differs from Agave: {key}");
        }
    }
}

fn accounts(bank: &Bank) -> Value {
    let mut rows = bank.get_all_accounts(true).unwrap();
    rows.sort_by_key(|(key, _, _)| *key);
    json!(rows.into_iter().map(|(key, a, _)| json!({
        "pubkey":key.to_string(), "lamports":a.lamports(), "owner":a.owner().to_string(),
        "executable":a.executable(), "rent_epoch":a.rent_epoch(), "data":STANDARD.encode(a.data())
    })).collect::<Vec<_>>())
}
fn state(bank: &Bank) -> Value {
    let lt = bank.calculate_accounts_lt_hash_for_tests();
    let fields = bank.get_fields_to_serialize();
    #[allow(deprecated)]
    let mut queue: Vec<_> = fields
        .blockhash_queue
        .get_recent_blockhashes()
        .map(|i| json!({"hash_index":i.0,"hash":i.1.to_string(),"lamports_per_signature":i.2}))
        .collect();
    queue.sort_by_key(|v| std::cmp::Reverse(v["hash_index"].as_u64().unwrap()));
    let mut epochs: Vec<_> = bank.epoch_stakes_map().iter().collect();
    epochs.sort_by_key(|(epoch, _)| **epoch);
    let epochs: Vec<_> = epochs.into_iter().map(|(epoch,s)| {
        let mut votes: Vec<_> = s.stakes().vote_accounts().iter().collect();
        votes.sort_by_key(|(key,_)| **key);
        json!({"epoch":epoch,"total_stake":s.total_stake(),"votes":votes.into_iter().map(|(key,a)| {
            let state = solana_vote_interface::state::VoteStateV4::deserialize(a.account().data(),key).unwrap();
            json!({"vote_account":key.to_string(),"node":a.node_pubkey().to_string(),"authorized_voter":s.epoch_authorized_voters().get(key).unwrap().to_string(),"stake":s.vote_account_stake(key),"lamports":a.account().lamports(),"bls_public_key":state.bls_pubkey_compressed.unwrap().iter().map(|b|format!("{b:02x}")).collect::<String>()})
        }).collect::<Vec<_>>()})
    }).collect();
    json!({"slot":bank.slot(), "bank_hash":bank.hash().to_string(),
        "last_blockhash":bank.last_blockhash().to_string(), "tick_height":bank.tick_height(),
        "capitalization":bank.capitalization(), "accounts_data_len":bank.calculate_accounts_data_size().unwrap(),
        "lthash":STANDARD.encode(bytemuck::cast_slice::<u16,u8>(&lt.0.0)),
        "accounts":accounts(bank), "epoch_stakes":epochs,
        "recent_blockhashes":queue,"blockhash_queue_max_age":fields.blockhash_queue.get_max_age(),
        "fees":fields.fee_rate_governor,"ticks_per_slot":fields.ticks_per_slot,"max_tick_height":fields.max_tick_height,"nanoseconds_per_slot":fields.ns_per_slot,
        "leader":bank.leader_id().to_string(),"slots_per_year":bank.slots_per_year(),
        "rent":bank.rent_collector().rent,"inflation":bank.inflation(),"epoch_schedule":bank.epoch_schedule(),
        "hashes_per_tick":bank.hashes_per_tick(),"lamports_per_signature":bank.get_lamports_per_signature(),
        "consensus_block_id":bank.block_id().map(|h|h.to_string()),
        "block_height":bank.block_height(), "signature_count":bank.signature_count(),
        "transaction_count":bank.transaction_count()})
}
fn main() {
    let args: Vec<String> = env::args().collect();
    if args[1] == "profile" {
        let mut g = GenesisConfig::default();
        g.epoch_schedule = solana_epoch_schedule::EpochSchedule::custom(
            solana_clock::DEFAULT_DEV_SLOTS_PER_EPOCH,
            solana_clock::DEFAULT_DEV_SLOTS_PER_EPOCH,
            false,
        );
        // Equivalent to the CLI's deterministic --hashes-per-tick sleep profile.
        g.poh_config.hashes_per_tick = None;
        genesis_utils::activate_all_features_alpenglow(&mut g);
        genesis_utils::add_genesis_stake_config_account(&mut g);
        genesis_utils::add_genesis_epoch_rewards_account(&mut g);
        g.creation_time = 0;
        fs::write(&args[2], bincode::serialize(&g).unwrap()).unwrap();
        let mut features: Vec<_> = agave_feature_set::FEATURE_NAMES
            .iter()
            .map(|(key, name)| json!({"address":key.to_string(),"name":name}))
            .collect();
        features.sort_by_key(|f| f["address"].as_str().unwrap().to_owned());
        fs::write(
            format!("{}.features.json", args[2]),
            serde_json::to_vec_pretty(&features).unwrap(),
        )
        .unwrap();
        let mut reserved: Vec<_> =
            agave_reserved_account_keys::ReservedAccountKeys::all_keys_iter()
                .map(|k| k.to_string())
                .collect();
        let (clock, _) = solana_pubkey::Pubkey::find_program_address(
            &[b"alpenclock"],
            &agave_feature_set::alpenglow::id(),
        );
        reserved.push(clock.to_string());
        reserved.sort();
        fs::write(
            format!("{}.reserved.json", args[2]),
            serde_json::to_vec_pretty(&reserved).unwrap(),
        )
        .unwrap();
        return;
    }
    let is_native = args[1] == "replay-native";
    let is_replay = args[1] == "replay" || is_native;
    let input_index = if is_replay { 2 } else { 1 };
    let bytes = fs::read(&args[input_index]).unwrap();
    let g: GenesisConfig = bincode::deserialize(&bytes).unwrap();
    assert_eq!(
        bincode::serialize(&g).unwrap(),
        bytes,
        "noncanonical genesis serialization"
    );
    validate_validator_accounts(&g);
    if is_native {
        replay_native(&g, &args[3], &args[4]);
        return;
    }
    if is_replay {
        replay(&g, &args[3]);
        return;
    }
    let bank = Bank::new_for_tests(&g);
    let initial = state(&bank);
    for entry in solana_entry::entry::create_ticks(
        g.ticks_per_slot,
        g.poh_config.hashes_per_tick.unwrap_or(0),
        g.hash(),
    ) {
        bank.register_tick_for_test(&entry.hash);
    }
    bank.freeze();
    let frozen = state(&bank);
    fs::write(&args[2], serde_json::to_vec_pretty(&json!({"agave_revision":"7e51da963aee49622a395f562386a6bd8ba0e717","genesis_hash":g.hash().to_string(),"initialized":initial,"frozen":frozen})).unwrap()).unwrap();
}
