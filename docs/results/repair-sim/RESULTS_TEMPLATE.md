# Repair simulation results template

Record the commit, CPU, Go version, governor/pinning, command, configuration,
and SHA-256 of every raw JSON file. Do not combine logical network time with
measured local CPU time.

| scenario | availability | slots | FEC/slot | completed | logical slots/s | CPU slots/s | requests | network data shreds | locally recovered shreds | FEC decodes | p50 completion | p95 completion | p99 completion |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| near-tip | near-loss | | | | | | | | | | | | |
| deep-catchup | mixed | | | | | | | | | | | | |
| deep-catchup | sparse | | | | | | | | | | | | |

Also report:

- `stage_cpu_ns` by stage;
- `repair_bytes_requested` and `repair_bytes_received`;
- canceled/late, duplicate, lost, and rejected-corrupt responses;
- queue high-water mark;
- spool bytes and complete slots;
- allocations;
- shred-signature cache hits and actual Ed25519 verifications;
- every limitation emitted in the JSON result.
