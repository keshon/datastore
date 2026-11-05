# Historical measurements

These figures appeared in the repository documentation before the 2026-11-05
revision. Hardware, Go version, measurement date and complete output were not
recorded alongside them. They remain historical observations, not current
performance or capacity guarantees.

## Previously published figures

| Operation | Reported observation |
|---|---|
| Commit, `SyncAlways` | fsync accounted for about 97% of elapsed time |
| Commit, `SyncNever` | About 30 microseconds |
| `Get` | About 2 microseconds |
| Full scan | About 2 microseconds per record; about 20 ms for 10,000 |
| `Open` | About 10 microseconds per record; about 100 ms for 10,000 |
| Index rebuilding | Described as taking about a millisecond |
| Group commit | One run reported 320 commits sharing 79 fsyncs |
| Race suite | About 30 seconds |
| Exhaustive recovery sweep | About three minutes |

For an index with 10,000 matches, taking the first ten was reported as 21 ms
and 100,000 allocations through `Find`, versus 1.4 ms and 99 allocations through
`All`. The relevant behavior is early termination: `Find` decodes the complete
set; `All` decodes per step. This does not promise a fixed speedup.

The published heap estimate was:

```text
heap ≈ payload + records × (90 + 145 × indexes) bytes
```

It described about 67 MB of overhead for 100,000 records with four indexes,
and heap-to-payload ratios of 7.4 at 127-byte records and 1.4 at 2 KB records.
These are fixture-specific estimates, not accounting performed by the library.
`Stats.Bytes` measures encoded payload and keys, excluding indexes and other
heap overhead. Do not use the formula as a memory limit.

## Reproduce with current fixtures

Run from the repository root:

```sh
go test -run '^$' -bench . -benchmem .
go test -run '^TestMemoryFootprint$' -count=1 -v .
go test -run '^TestGroupCommitSharesOneFsync$' -count=1 -v .
```

Record the commit, Go version, OS, CPU, filesystem, storage device, fixture and
complete output with new measurements. The group-commit test checks sharing;
it does not require the historical fsync count. See
[conventions](../conventions.md) for correctness checks and
[the guide](../guide.md#capacity-and-observability) for capacity monitoring.
