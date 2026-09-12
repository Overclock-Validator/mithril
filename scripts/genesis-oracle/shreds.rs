// Independent wire oracle, compiled inside the pinned Agave ledger crate.
// Input is a small length-prefixed test protocol (see genesis_ingress_test.go).
use solana_entry::{block_component::BlockComponent, entry::create_ticks};
use solana_genesis_config::GenesisConfig;
use solana_hash::Hash;
use solana_keypair::keypair_from_seed;
use solana_ledger::shred::{
    layout, recover, ProcessShredsStats, ReedSolomonCache, Shred, Shredder,
};
use solana_signer::Signer;
use std::{
    collections::HashSet,
    env, fs,
    io::{Cursor, Read},
};

fn bytes<const N: usize>(r: &mut Cursor<Vec<u8>>) -> [u8; N] {
    let mut b = [0; N];
    r.read_exact(&mut b).unwrap();
    b
}
fn u32le(r: &mut Cursor<Vec<u8>>) -> u32 {
    u32::from_le_bytes(bytes(r))
}
fn blob(r: &mut Cursor<Vec<u8>>) -> Vec<u8> {
    let len = u32le(r) as usize;
    assert!(len <= 1_000_000);
    let mut b = vec![0; len];
    r.read_exact(&mut b).unwrap();
    b
}
fn main() {
    let args: Vec<_> = env::args().collect();
    let g: GenesisConfig = bincode::deserialize(&fs::read(&args[1]).unwrap()).unwrap();
    let mut input = Cursor::new(fs::read(&args[2]).unwrap());
    assert_eq!(&bytes::<8>(&mut input), b"MSHRED01");
    let version = u16::from_le_bytes(bytes(&mut input));
    assert_eq!(
        version,
        solana_shred_version::compute_shred_version(&g.hash(), None)
    );
    let key = keypair_from_seed(&[1; 32]).unwrap();
    let cache = ReedSolomonCache::default();
    let batches = u32le(&mut input);
    let mut packet_count = 0;
    let mut genesis_root = Hash::default();
    let mut previous_root = g.hash();
    for batch in 0..batches {
        let slot = u64::from_le_bytes(bytes(&mut input));
        let parent = u64::from_le_bytes(bytes(&mut input));
        let data_index = u32le(&mut input);
        let code_index = u32le(&mut input);
        let last = bytes::<1>(&mut input)[0] != 0;
        let chain = Hash::new_from_array(bytes(&mut input));
        assert_eq!(
            chain, previous_root,
            "broken FEC/slot chain in batch {batch}"
        );
        let encoded = blob(&mut input);
        let component: BlockComponent = wincode::deserialize(&encoded).unwrap();
        assert_eq!(wincode::serialize(&component).unwrap(), encoded);
        if batch == 0 {
            assert_eq!((slot, parent, data_index, code_index), (0, 0, 0, 0));
            assert!(last);
            assert_eq!(chain, g.hash());
            assert_eq!(
                encoded,
                bincode::serialize(&create_ticks(
                    g.ticks_per_slot,
                    g.poh_config.hashes_per_tick.unwrap_or(0),
                    g.hash()
                ))
                .unwrap()
            );
        }
        let (mut expected_data, mut expected_code) = Shredder::new(slot, parent, 0, version)
            .unwrap()
            .component_to_merkle_shreds_for_tests(
                &key,
                &component,
                last,
                chain,
                data_index,
                code_index,
                &cache,
                &mut ProcessShredsStats::default(),
            );
        // Agave's shredder leaves the retransmitter-signature trailer blank;
        // Mithril's broadcaster also signs the leader's first Turbine hop.
        // Apply Agave's actual hop-signing API before comparing the wire bytes.
        for shred in expected_data.iter_mut().chain(&mut expected_code) {
            if layout::is_retransmitter_signed_variant(shred.payload()).unwrap() {
                let mut packet = shred.payload().to_vec();
                layout::resign_shred(&mut packet, &key).unwrap();
                *shred = Shred::new_from_serialized_shred(packet).unwrap();
            }
        }
        let count = u32le(&mut input) as usize;
        assert_eq!(count, expected_data.len() + expected_code.len());
        let mut shreds = Vec::new();
        let mut seen = HashSet::new();
        for _ in 0..count {
            let packet = blob(&mut input);
            let shred = Shred::new_from_serialized_shred(packet.clone()).unwrap();
            shred.sanitize().unwrap();
            assert!(shred.verify(&key.pubkey()));
            assert_eq!(shred.slot(), slot);
            assert_eq!(shred.version(), version);
            assert!(seen.insert(shred.id()), "duplicate shred in oracle input");
            let expected = expected_data
                .iter()
                .chain(&expected_code)
                .find(|s| s.id() == shred.id())
                .unwrap();
            assert!(
                packet.as_slice() == expected.payload().as_ref(),
                "batch {batch}, shred {:?}: first differing byte {:?}",
                shred.id(),
                packet
                    .iter()
                    .zip(expected.payload().iter())
                    .position(|(a, b)| a != b)
            );
            shreds.push(shred);
        }
        // These small fixtures occupy one FEC set per component. Independently
        // recover a missing data shred using Agave's parity and proof builder.
        assert_eq!(expected_data.len(), 32);
        let recovered: Vec<_> = recover(
            shreds
                .into_iter()
                .filter(|s| s.id() != expected_data[0].id()),
            &cache,
        )
        .unwrap()
        .map(Result::unwrap)
        .collect();
        let first = recovered
            .iter()
            .find(|s| s.id() == expected_data[0].id())
            .unwrap();
        assert!(first.verify(&key.pubkey()));
        assert_eq!(first.payload(), expected_data[0].payload());
        let deshredded = Shredder::deshred(expected_data.iter().map(Shred::payload)).unwrap();
        assert!(deshredded.starts_with(&encoded));
        assert!(deshredded[encoded.len()..].iter().all(|b| *b == 0));
        if batch == 0 {
            genesis_root = expected_data.last().unwrap().merkle_root().unwrap();
        }
        previous_root = expected_data.last().unwrap().merkle_root().unwrap();
        packet_count += count;
    }
    assert_eq!(input.position(), input.get_ref().len() as u64);
    println!("agave_revision=7e51da963aee49622a395f562386a6bd8ba0e717\ncomponents={batches}\nshreds={packet_count}\nversion={version}\ngenesis_chained_root={genesis_root}");
}
