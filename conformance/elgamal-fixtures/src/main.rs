// Proof inputs ported from Agave ffa314a0e10f48019cace790f633199a7c782024,
// programs/zk-elgamal-proof-tests/tests/process_transaction.rs (Apache-2.0).
// Generate with Agave's pinned SDK; Go tests consume the checked-in JSON.
use bytemuck::{bytes_of, bytes_of_mut, pod_read_unaligned, Pod};
use serde_json::{json, Value};
use solana_zk_elgamal_proof_interface::{instruction::ProofInstruction, proof_data::*};
use solana_zk_sdk::{
    encryption::{
        elgamal::{ElGamalKeypair, ElGamalPubkey, ElGamalSecretKey},
        grouped_elgamal::GroupedElGamal,
        pedersen::{Pedersen, PedersenOpening},
    },
    zk_elgamal_proof_program::*,
};

fn emit<T, U>(
    out: &mut Vec<Value>,
    name: &str,
    instruction: ProofInstruction,
    data: &T,
    expected: bool,
) where
    T: Pod + ZkProofData<U> + VerifyZkProof,
    U: Pod,
{
    let valid = data.verify_proof().is_ok();
    assert_eq!(valid, expected, "SDK oracle disagrees: {name}");
    let mut wire = vec![instruction as u8];
    wire.extend_from_slice(bytes_of(data));
    out.push(json!({"name":name,"instruction":instruction as u8,"valid":valid,"context_size":bytes_of(data.context_data()).len(),"data_hex":hex::encode(wire)}));
    if valid && (name.starts_with("agave/") || name == "sdk/percentage/below_cap") {
        mutations(out, name, instruction, data);
    }
}

// Every valid upstream case also exercises canonical decoding and forbidden
// identity points. Expected results are obtained from Rust, never from Go.
fn mutations<T, U>(out: &mut Vec<Value>, name: &str, instruction: ProofInstruction, data: &T)
where
    T: Pod + ZkProofData<U> + VerifyZkProof,
    U: Pod,
{
    let context_len = bytes_of(data.context_data()).len();
    for offset in (0..bytes_of(data).len() - 31).step_by(32) {
        for (kind, fill) in [("identity_or_zero", 0u8), ("noncanonical", 255u8)] {
            let mut changed = *data;
            bytes_of_mut(&mut changed)[offset..offset + 32].fill(fill);
            let valid = changed.verify_proof().is_ok();
            let mut wire = vec![instruction as u8];
            wire.extend_from_slice(bytes_of(&changed));
            out.push(json!({"name":format!("mutation/{name}/{offset}/{kind}"),"instruction":instruction as u8,"valid":valid,"context_size":context_len,"data_hex":hex::encode(wire)}));
        }
    }
}

fn test_zero_balance(out: &mut Vec<Value>) {
    let elgamal_keypair = ElGamalKeypair::new_rand();

    let zero_ciphertext = elgamal_keypair.pubkey().encrypt(0_u64);
    let success_proof_data =
        build_zero_ciphertext_proof_data(&elgamal_keypair, &zero_ciphertext).unwrap();

    let mut fail_proof_context = success_proof_data.context;
    fail_proof_context.pubkey = ElGamalPubkey::default().into();
    let fail_proof_data = ZeroCiphertextProofData {
        context: fail_proof_context,
        proof: success_proof_data.proof,
    };

    emit(
        out,
        "agave/test_zero_balance/valid",
        ProofInstruction::VerifyZeroCiphertext,
        &success_proof_data,
        true,
    );
    emit(
        out,
        "agave/test_zero_balance/invalid",
        ProofInstruction::VerifyZeroCiphertext,
        &fail_proof_data,
        false,
    );
}

fn test_ciphertext_ciphertext_equality(out: &mut Vec<Value>) {
    let source_keypair = ElGamalKeypair::new_rand();
    let destination_keypair = ElGamalKeypair::new_rand();

    let amount: u64 = 0;
    let source_ciphertext = source_keypair.pubkey().encrypt(amount);

    let destination_opening = PedersenOpening::new_rand();
    let destination_ciphertext = destination_keypair
        .pubkey()
        .encrypt_with(amount, &destination_opening);

    let success_proof_data = build_ciphertext_ciphertext_equality_proof_data(
        &source_keypair,
        destination_keypair.pubkey(),
        &source_ciphertext,
        &destination_ciphertext,
        &destination_opening,
        amount,
    )
    .unwrap();

    let mut fail_proof_context = success_proof_data.context;
    fail_proof_context.first_pubkey = ElGamalPubkey::default().into();
    let fail_proof_data = CiphertextCiphertextEqualityProofData {
        context: fail_proof_context,
        proof: success_proof_data.proof,
    };

    emit(
        out,
        "agave/test_ciphertext_ciphertext_equality/valid",
        ProofInstruction::VerifyCiphertextCiphertextEquality,
        &success_proof_data,
        true,
    );
    emit(
        out,
        "agave/test_ciphertext_ciphertext_equality/invalid",
        ProofInstruction::VerifyCiphertextCiphertextEquality,
        &fail_proof_data,
        false,
    );
}

fn test_pubkey_validity(out: &mut Vec<Value>) {
    let elgamal_keypair = ElGamalKeypair::new_rand();

    let success_proof_data = build_pubkey_validity_proof_data(&elgamal_keypair).unwrap();

    let incorrect_pubkey = elgamal_keypair.pubkey();
    let incorrect_secret = ElGamalSecretKey::new_rand();
    let incorrect_keypair = ElGamalKeypair::new_for_tests(*incorrect_pubkey, incorrect_secret);

    let fail_proof_data = build_pubkey_validity_proof_data(&incorrect_keypair).unwrap();

    emit(
        out,
        "agave/test_pubkey_validity/valid",
        ProofInstruction::VerifyPubkeyValidity,
        &success_proof_data,
        true,
    );
    emit(
        out,
        "agave/test_pubkey_validity/invalid",
        ProofInstruction::VerifyPubkeyValidity,
        &fail_proof_data,
        false,
    );
}

fn test_batched_range_proof_u64(out: &mut Vec<Value>) {
    let amount_1 = 23_u64;
    let amount_2 = 24_u64;

    let (commitment_1, opening_1) = Pedersen::new(amount_1);
    let (commitment_2, opening_2) = Pedersen::new(amount_2);

    let success_proof_data = build_batched_range_proof_u64_data(
        vec![&commitment_1, &commitment_2],
        vec![amount_1, amount_2],
        vec![32, 32],
        vec![&opening_1, &opening_2],
    )
    .unwrap();

    let incorrect_opening = PedersenOpening::new_rand();
    let fail_proof_data = build_batched_range_proof_u64_data(
        vec![&commitment_1, &commitment_2],
        vec![amount_1, amount_2],
        vec![32, 32],
        vec![&opening_1, &incorrect_opening],
    )
    .unwrap();

    emit(
        out,
        "agave/test_batched_range_proof_u64/valid",
        ProofInstruction::VerifyBatchedRangeProofU64,
        &success_proof_data,
        true,
    );
    emit(
        out,
        "agave/test_batched_range_proof_u64/invalid",
        ProofInstruction::VerifyBatchedRangeProofU64,
        &fail_proof_data,
        false,
    );
}

fn test_batched_range_proof_u128(out: &mut Vec<Value>) {
    let amount_1 = 23_u64;
    let amount_2 = 24_u64;

    let (commitment_1, opening_1) = Pedersen::new(amount_1);
    let (commitment_2, opening_2) = Pedersen::new(amount_2);

    let success_proof_data = build_batched_range_proof_u128_data(
        vec![&commitment_1, &commitment_2],
        vec![amount_1, amount_2],
        vec![64, 64],
        vec![&opening_1, &opening_2],
    )
    .unwrap();

    let incorrect_opening = PedersenOpening::new_rand();
    let fail_proof_data = build_batched_range_proof_u128_data(
        vec![&commitment_1, &commitment_2],
        vec![amount_1, amount_2],
        vec![64, 64],
        vec![&opening_1, &incorrect_opening],
    )
    .unwrap();

    emit(
        out,
        "agave/test_batched_range_proof_u128/valid",
        ProofInstruction::VerifyBatchedRangeProofU128,
        &success_proof_data,
        true,
    );
    emit(
        out,
        "agave/test_batched_range_proof_u128/invalid",
        ProofInstruction::VerifyBatchedRangeProofU128,
        &fail_proof_data,
        false,
    );
}

fn test_batched_range_proof_u256(out: &mut Vec<Value>) {
    let amount_1 = 23_u64;
    let amount_2 = 24_u64;
    let amount_3 = 25_u64;
    let amount_4 = 26_u64;

    let (commitment_1, opening_1) = Pedersen::new(amount_1);
    let (commitment_2, opening_2) = Pedersen::new(amount_2);
    let (commitment_3, opening_3) = Pedersen::new(amount_3);
    let (commitment_4, opening_4) = Pedersen::new(amount_4);

    let success_proof_data = build_batched_range_proof_u256_data(
        vec![&commitment_1, &commitment_2, &commitment_3, &commitment_4],
        vec![amount_1, amount_2, amount_3, amount_4],
        vec![64, 64, 64, 64],
        vec![&opening_1, &opening_2, &opening_3, &opening_4],
    )
    .unwrap();

    let incorrect_opening = PedersenOpening::new_rand();
    let fail_proof_data = build_batched_range_proof_u256_data(
        vec![&commitment_1, &commitment_2, &commitment_3, &commitment_4],
        vec![amount_1, amount_2, amount_3, amount_4],
        vec![64, 64, 64, 64],
        vec![&opening_1, &opening_2, &opening_3, &incorrect_opening],
    )
    .unwrap();

    emit(
        out,
        "agave/test_batched_range_proof_u256/valid",
        ProofInstruction::VerifyBatchedRangeProofU256,
        &success_proof_data,
        true,
    );
    emit(
        out,
        "agave/test_batched_range_proof_u256/invalid",
        ProofInstruction::VerifyBatchedRangeProofU256,
        &fail_proof_data,
        false,
    );
}

fn test_ciphertext_commitment_equality(out: &mut Vec<Value>) {
    let keypair = ElGamalKeypair::new_rand();
    let amount: u64 = 55;
    let ciphertext = keypair.pubkey().encrypt(amount);
    let (commitment, opening) = Pedersen::new(amount);

    let success_proof_data = build_ciphertext_commitment_equality_proof_data(
        &keypair,
        &ciphertext,
        &commitment,
        &opening,
        amount,
    )
    .unwrap();

    let mut fail_proof_context = success_proof_data.context;
    fail_proof_context.pubkey = ElGamalPubkey::default().into();
    let fail_proof_data = CiphertextCommitmentEqualityProofData {
        context: fail_proof_context,
        proof: success_proof_data.proof,
    };

    emit(
        out,
        "agave/test_ciphertext_commitment_equality/valid",
        ProofInstruction::VerifyCiphertextCommitmentEquality,
        &success_proof_data,
        true,
    );
    emit(
        out,
        "agave/test_ciphertext_commitment_equality/invalid",
        ProofInstruction::VerifyCiphertextCommitmentEquality,
        &fail_proof_data,
        false,
    );
}

fn test_grouped_ciphertext_2_handles_validity(out: &mut Vec<Value>) {
    let destination_keypair = ElGamalKeypair::new_rand();
    let destination_pubkey = destination_keypair.pubkey();

    let auditor_keypair = ElGamalKeypair::new_rand();
    let auditor_pubkey = auditor_keypair.pubkey();

    let amount: u64 = 55;
    let opening = PedersenOpening::new_rand();
    let grouped_ciphertext =
        GroupedElGamal::encrypt_with([destination_pubkey, auditor_pubkey], amount, &opening);

    let success_proof_data = build_grouped_ciphertext_2_handles_validity_proof_data(
        destination_pubkey,
        auditor_pubkey,
        &grouped_ciphertext,
        amount,
        &opening,
    )
    .unwrap();

    let mut fail_proof_context = success_proof_data.context;
    fail_proof_context.first_pubkey = ElGamalPubkey::default().into();
    let fail_proof_data = GroupedCiphertext2HandlesValidityProofData {
        context: fail_proof_context,
        proof: success_proof_data.proof,
    };

    emit(
        out,
        "agave/test_grouped_ciphertext_2_handles_validity/valid",
        ProofInstruction::VerifyGroupedCiphertext2HandlesValidity,
        &success_proof_data,
        true,
    );
    emit(
        out,
        "agave/test_grouped_ciphertext_2_handles_validity/invalid",
        ProofInstruction::VerifyGroupedCiphertext2HandlesValidity,
        &fail_proof_data,
        false,
    );
}

fn test_batched_grouped_ciphertext_2_handles_validity(out: &mut Vec<Value>) {
    let destination_keypair = ElGamalKeypair::new_rand();
    let destination_pubkey = destination_keypair.pubkey();

    let auditor_keypair = ElGamalKeypair::new_rand();
    let auditor_pubkey = auditor_keypair.pubkey();

    let amount_lo: u64 = 55;
    let amount_hi: u64 = 22;

    let opening_lo = PedersenOpening::new_rand();
    let opening_hi = PedersenOpening::new_rand();

    let grouped_ciphertext_lo =
        GroupedElGamal::encrypt_with([destination_pubkey, auditor_pubkey], amount_lo, &opening_lo);
    let grouped_ciphertext_hi =
        GroupedElGamal::encrypt_with([destination_pubkey, auditor_pubkey], amount_hi, &opening_hi);

    let success_proof_data = build_batched_grouped_ciphertext_2_handles_validity_proof_data(
        destination_pubkey,
        auditor_pubkey,
        &grouped_ciphertext_lo,
        &grouped_ciphertext_hi,
        amount_lo,
        amount_hi,
        &opening_lo,
        &opening_hi,
    )
    .unwrap();

    let mut fail_proof_context = success_proof_data.context;
    fail_proof_context.first_pubkey = ElGamalPubkey::default().into();
    let fail_proof_data = BatchedGroupedCiphertext2HandlesValidityProofData {
        context: fail_proof_context,
        proof: success_proof_data.proof,
    };

    emit(
        out,
        "agave/test_batched_grouped_ciphertext_2_handles_validity/valid",
        ProofInstruction::VerifyBatchedGroupedCiphertext2HandlesValidity,
        &success_proof_data,
        true,
    );
    emit(
        out,
        "agave/test_batched_grouped_ciphertext_2_handles_validity/invalid",
        ProofInstruction::VerifyBatchedGroupedCiphertext2HandlesValidity,
        &fail_proof_data,
        false,
    );
}

fn test_grouped_ciphertext_3_handles_validity(out: &mut Vec<Value>) {
    let source_keypair = ElGamalKeypair::new_rand();
    let source_pubkey = source_keypair.pubkey();

    let destination_keypair = ElGamalKeypair::new_rand();
    let destination_pubkey = destination_keypair.pubkey();

    let auditor_keypair = ElGamalKeypair::new_rand();
    let auditor_pubkey = auditor_keypair.pubkey();

    let amount: u64 = 55;
    let opening = PedersenOpening::new_rand();
    let grouped_ciphertext = GroupedElGamal::encrypt_with(
        [source_pubkey, destination_pubkey, auditor_pubkey],
        amount,
        &opening,
    );

    let success_proof_data = build_grouped_ciphertext_3_handles_validity_proof_data(
        source_pubkey,
        destination_pubkey,
        auditor_pubkey,
        &grouped_ciphertext,
        amount,
        &opening,
    )
    .unwrap();

    let mut fail_proof_context = success_proof_data.context;
    fail_proof_context.first_pubkey = ElGamalPubkey::default().into();
    let fail_proof_data = GroupedCiphertext3HandlesValidityProofData {
        context: fail_proof_context,
        proof: success_proof_data.proof,
    };

    emit(
        out,
        "agave/test_grouped_ciphertext_3_handles_validity/valid",
        ProofInstruction::VerifyGroupedCiphertext3HandlesValidity,
        &success_proof_data,
        true,
    );
    emit(
        out,
        "agave/test_grouped_ciphertext_3_handles_validity/invalid",
        ProofInstruction::VerifyGroupedCiphertext3HandlesValidity,
        &fail_proof_data,
        false,
    );
}

fn test_batched_grouped_ciphertext_3_handles_validity(out: &mut Vec<Value>) {
    let source_keypair = ElGamalKeypair::new_rand();
    let source_pubkey = source_keypair.pubkey();

    let destination_keypair = ElGamalKeypair::new_rand();
    let destination_pubkey = destination_keypair.pubkey();

    let auditor_keypair = ElGamalKeypair::new_rand();
    let auditor_pubkey = auditor_keypair.pubkey();

    let amount_lo: u64 = 55;
    let amount_hi: u64 = 22;

    let opening_lo = PedersenOpening::new_rand();
    let opening_hi = PedersenOpening::new_rand();

    let grouped_ciphertext_lo = GroupedElGamal::encrypt_with(
        [source_pubkey, destination_pubkey, auditor_pubkey],
        amount_lo,
        &opening_lo,
    );
    let grouped_ciphertext_hi = GroupedElGamal::encrypt_with(
        [source_pubkey, destination_pubkey, auditor_pubkey],
        amount_hi,
        &opening_hi,
    );

    let success_proof_data = build_batched_grouped_ciphertext_3_handles_validity_proof_data(
        source_pubkey,
        destination_pubkey,
        auditor_pubkey,
        &grouped_ciphertext_lo,
        &grouped_ciphertext_hi,
        amount_lo,
        amount_hi,
        &opening_lo,
        &opening_hi,
    )
    .unwrap();

    let mut fail_proof_context = success_proof_data.context;
    fail_proof_context.first_pubkey = ElGamalPubkey::default().into();
    let fail_proof_data = BatchedGroupedCiphertext3HandlesValidityProofData {
        context: fail_proof_context,
        proof: success_proof_data.proof,
    };

    emit(
        out,
        "agave/test_batched_grouped_ciphertext_3_handles_validity/valid",
        ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity,
        &success_proof_data,
        true,
    );
    emit(
        out,
        "agave/test_batched_grouped_ciphertext_3_handles_validity/invalid",
        ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity,
        &fail_proof_data,
        false,
    );
}

fn percentage(out: &mut Vec<Value>) {
    // Port SDK percentage_with_cap::test_percentage_with_cap_instruction_correctness
    // and test_percentage_with_cap_instruction_inconsistent_delta_amount.
    let (base, base_opening) = Pedersen::new(1_u64);
    let (percentage, percentage_opening) = Pedersen::new(1_u64);
    let delta = &percentage * 10_000_u64 - &base * 400_u64;
    let delta_opening = &percentage_opening * 10_000_u64 - &base_opening * 400_u64;
    let (claimed, claimed_opening) = Pedersen::new(9600_u64);
    let data = build_percentage_with_cap_proof_data(
        &percentage,
        &percentage_opening,
        1,
        &delta,
        &delta_opening,
        9600,
        &claimed,
        &claimed_opening,
        3,
    )
    .unwrap();
    emit(
        out,
        "sdk/percentage/below_cap",
        ProofInstruction::VerifyPercentageWithCap,
        &data,
        true,
    );
    let mut bad = data;
    bad.context.percentage_commitment = Pedersen::new(999_u64).0.into();
    emit(
        out,
        "sdk/percentage/below_cap/invalid",
        ProofInstruction::VerifyPercentageWithCap,
        &bad,
        false,
    );

    for (name, amount, claimed_amount) in [("at_cap", 3_u64, 100_u64), ("max_u64", u64::MAX, 100)] {
        let (percentage, percentage_opening) = Pedersen::new(amount);
        let (delta, delta_opening) = Pedersen::new(claimed_amount);
        let (claimed, claimed_opening) = Pedersen::new(claimed_amount);
        let data = build_percentage_with_cap_proof_data(
            &percentage,
            &percentage_opening,
            amount,
            &delta,
            &delta_opening,
            claimed_amount,
            &claimed,
            &claimed_opening,
            amount,
        )
        .unwrap();
        emit(
            out,
            &format!("sdk/percentage/{name}"),
            ProofInstruction::VerifyPercentageWithCap,
            &data,
            true,
        );
        let mut bad = data;
        bad.context.percentage_commitment = Pedersen::new(999_u64).0.into();
        emit(
            out,
            &format!("sdk/percentage/{name}/invalid"),
            ProofInstruction::VerifyPercentageWithCap,
            &bad,
            false,
        );
    }

    let (transfer, transfer_opening) = Pedersen::new(100_u64);
    let (percentage, percentage_opening) = Pedersen::new(3_u64);
    let delta = &percentage * 10_000_u64 - &transfer * 400_u64;
    let delta_opening = &percentage_opening * 10_000_u64 - &transfer_opening * 400_u64;
    let (claimed, claimed_opening) = Pedersen::new(0_u64);
    let data = build_percentage_with_cap_proof_data(
        &percentage,
        &percentage_opening,
        3,
        &delta,
        &delta_opening,
        0,
        &claimed,
        &claimed_opening,
        3,
    )
    .unwrap();
    // At the cap, the alternate OR-proof branch need not match delta.
    emit(
        out,
        "sdk/percentage/at_cap_inconsistent_delta",
        ProofInstruction::VerifyPercentageWithCap,
        &data,
        true,
    );
}

fn edge_cases(out: &mut Vec<Value>) {
    // Non-power-of-two component widths, single and maximum-size batches.
    for (name, bits) in [
        ("one", vec![64]),
        ("non_power_of_two", vec![1, 2, 3, 4, 5, 6, 7, 36]),
        ("eight", vec![8; 8]),
    ] {
        let pairs: Vec<_> = bits.iter().map(|_| Pedersen::new(1u64)).collect();
        let data = build_batched_range_proof_u64_data(
            pairs.iter().map(|p| &p.0).collect(),
            vec![1; bits.len()],
            bits,
            pairs.iter().map(|p| &p.1).collect(),
        )
        .unwrap();
        emit(
            out,
            &format!("sdk/range64/{name}"),
            ProofInstruction::VerifyBatchedRangeProofU64,
            &data,
            true,
        );
        if name == "one" {
            let mut bad = data;
            bad.context.bit_lengths[7] = 1;
            emit(
                out,
                "sdk/range64/nonzero_bit_padding",
                ProofInstruction::VerifyBatchedRangeProofU64,
                &bad,
                false,
            );
            let mut bad = data;
            bad.context.commitments[7] = data.context.commitments[0];
            emit(
                out,
                "sdk/range64/nonzero_commitment_padding",
                ProofInstruction::VerifyBatchedRangeProofU64,
                &bad,
                false,
            );
        }
    }
    // An absent auditor (identity public key) is supported in both grouped formats.
    let a = ElGamalKeypair::new_rand();
    let b = ElGamalKeypair::new_rand();
    let auditor = ElGamalPubkey::default();
    let lo = PedersenOpening::new_rand();
    let hi = PedersenOpening::new_rand();
    let c2lo = GroupedElGamal::encrypt_with([a.pubkey(), &auditor], 55u64, &lo);
    let c2hi = GroupedElGamal::encrypt_with([a.pubkey(), &auditor], 22u64, &hi);
    let data = build_grouped_ciphertext_2_handles_validity_proof_data(
        a.pubkey(),
        &auditor,
        &c2lo,
        55,
        &lo,
    )
    .unwrap();
    emit(
        out,
        "sdk/grouped2/absent_auditor",
        ProofInstruction::VerifyGroupedCiphertext2HandlesValidity,
        &data,
        true,
    );
    let data = build_batched_grouped_ciphertext_2_handles_validity_proof_data(
        a.pubkey(),
        &auditor,
        &c2lo,
        &c2hi,
        55,
        22,
        &lo,
        &hi,
    )
    .unwrap();
    emit(
        out,
        "sdk/batched_grouped2/absent_auditor",
        ProofInstruction::VerifyBatchedGroupedCiphertext2HandlesValidity,
        &data,
        true,
    );
    let c3lo = GroupedElGamal::encrypt_with([a.pubkey(), b.pubkey(), &auditor], 55u64, &lo);
    let c3hi = GroupedElGamal::encrypt_with([a.pubkey(), b.pubkey(), &auditor], 22u64, &hi);
    let data = build_grouped_ciphertext_3_handles_validity_proof_data(
        a.pubkey(),
        b.pubkey(),
        &auditor,
        &c3lo,
        55,
        &lo,
    )
    .unwrap();
    emit(
        out,
        "sdk/grouped3/absent_auditor",
        ProofInstruction::VerifyGroupedCiphertext3HandlesValidity,
        &data,
        true,
    );
    let data = build_batched_grouped_ciphertext_3_handles_validity_proof_data(
        a.pubkey(),
        b.pubkey(),
        &auditor,
        &c3lo,
        &c3hi,
        55,
        22,
        &lo,
        &hi,
    )
    .unwrap();
    emit(
        out,
        "sdk/batched_grouped3/absent_auditor",
        ProofInstruction::VerifyBatchedGroupedCiphertext3HandlesValidity,
        &data,
        true,
    );
    // Equality permits an identity second ciphertext; first ciphertext remains nonzero.
    let zero = PedersenOpening::new(curve25519_dalek::scalar::Scalar::ZERO);
    let first = a.pubkey().encrypt(0u64);
    let second = b.pubkey().encrypt_with(0u64, &zero);
    let data =
        build_ciphertext_ciphertext_equality_proof_data(&a, b.pubkey(), &first, &second, &zero, 0)
            .unwrap();
    emit(
        out,
        "sdk/equality/identity_second_ciphertext",
        ProofInstruction::VerifyCiphertextCiphertextEquality,
        &data,
        true,
    );
}

fn legacy_vectors(out: &mut Vec<Value>) {
    // Keep the historical fixtures as rejection regressions, checked by this SDK.
    for (i, line) in include_str!("../../../pkg/sealevel/el_gamal_test.go")
        .lines()
        .filter(|l| l.contains("hex.DecodeString("))
        .enumerate()
    {
        let wire = hex::decode(line.split('"').nth(1).unwrap()).unwrap();
        macro_rules! check {
            ($ty:ty,$ix:ident) => {{
                let data: $ty = pod_read_unaligned(&wire[1..]);
                emit(
                    out,
                    &format!("legacy/{i}"),
                    ProofInstruction::$ix,
                    &data,
                    false,
                );
            }};
        }
        match wire[0] {
            3 => check!(
                CiphertextCommitmentEqualityProofData,
                VerifyCiphertextCommitmentEquality
            ),
            4 => check!(PubkeyValidityProofData, VerifyPubkeyValidity),
            6 => check!(BatchedRangeProofU64Data, VerifyBatchedRangeProofU64),
            7 => check!(BatchedRangeProofU128Data, VerifyBatchedRangeProofU128),
            _ => panic!("unexpected legacy type"),
        }
    }
}

fn captured_testnet(out: &mut Vec<Value>) {
    // Actual VerifyPubkeyValidity instructions from the two rejected transactions
    // in testnet slot 447765040. SDK verification is independent of Mithril.
    for (index,hex) in [
 (2,"04521ea87b343534c5f7fd931c2c6c925c43ef730bc6a3161c11b9edad5c314c7e0ce7d64e5cb679daa2081705d55300a4fc6569997942da8a01ae70c69b986f329f5e75ce7b485be0ce140f04cc1621a7e427930fae4beffef6d43870ca3dce04"),
 (4,"04521ea87b343534c5f7fd931c2c6c925c43ef730bc6a3161c11b9edad5c314c7e4af0c6b4dedc5de2dab23e58e81cf0211724849efc88292d7ceaf3f1fe5b4e112989a2bda2dbed41b2cb948a140851f727c89eb1dca8f7b210f44b445232f70a")
 ] {
  let wire=hex::decode(hex).unwrap();
  let data: PubkeyValidityProofData=pod_read_unaligned(&wire[1..]);
  emit(out,&format!("testnet/447765040/transaction_{index}"),ProofInstruction::VerifyPubkeyValidity,&data,true);
 }
}

fn main() {
    let mut out = Vec::new();
    test_zero_balance(&mut out);
    test_ciphertext_ciphertext_equality(&mut out);
    test_pubkey_validity(&mut out);
    test_batched_range_proof_u64(&mut out);
    test_batched_range_proof_u128(&mut out);
    test_batched_range_proof_u256(&mut out);
    test_ciphertext_commitment_equality(&mut out);
    test_grouped_ciphertext_2_handles_validity(&mut out);
    test_batched_grouped_ciphertext_2_handles_validity(&mut out);
    test_grouped_ciphertext_3_handles_validity(&mut out);
    test_batched_grouped_ciphertext_3_handles_validity(&mut out);
    percentage(&mut out);
    edge_cases(&mut out);
    captured_testnet(&mut out);
    legacy_vectors(&mut out);
    println!("{}",serde_json::to_string_pretty(&json!({"agave_commit":"ffa314a0e10f48019cace790f633199a7c782024","sdk":"8.0.1","cases":out})).unwrap());
}
