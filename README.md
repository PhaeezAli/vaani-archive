# The Archive prototype

This is a small prototype of the parts of my design for the Archive that I trusted least: the acknowledgement path, fencing, recovery, the data model, the segment format and similar stations. It is not a full system. It runs one node, and a local directory with S3's semantics stands in for object storage. It is about [MEASURED: N, wc -l of archive/] lines of library code and [MEASURED: M, wc -l of tests/] lines of tests, harness included. The design took about four hours. The prototype came after, to test it.

## Run it

It needs macOS or Linux (the store uses fcntl) and Python 3.13. A plain pip install fails on Homebrew Python, so this is the only install line I give.

```
python3.13 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python demo.py
.venv/bin/python -m pytest -q
```

The demo starts the real server in its own process, kills it with SIGKILL during ingest, restarts it, checks every station against a dictionary of what was acknowledged, and exits 1 if any check fails. It keeps its work directory and prints the path, so you can open the real WAL objects, segments and manifest.json under store/.

## A real run

This is the demo output from the final commit [MEASURED: hash] on [MEASURED: date], unedited. The suite passed 5 runs back to back and the kill test 20 runs back to back on [MEASURED: date].

```
$ .venv/bin/python demo.py
work directory kept for inspection: [MEASURED: path]

[  0.0s] kill scenario: 24 stations, 2 days of 30-minute readings, 5 channels, 8 clients
[MEASURED: s] measurements

check                                              result                                     ok
kill run was decisive                              [MEASURED: acked, outstanding, unknown, segments, WAL tail rows]  [MEASURED]
acknowledged readings missing right after restart  [MEASURED: 0 of readings]                  [MEASURED]
acknowledged readings returned twice               [MEASURED: 0]                              [MEASURED]
stations matching the dict oracle after resends    [MEASURED: 24 of 24]                       [MEASURED]
batches per WAL put, 8 clients, 200 ms window      [MEASURED: group]                          [MEASURED]

measurement                                              value
requests outstanding at the kill                         [MEASURED]
WAL objects replayed after the kill                      [MEASURED]
readings sent including resends and corrections          [MEASURED]
server peak RSS during the kill run                      [MEASURED] MB
durable 8 KB put with F_FULLFSYNC, median of 40          [MEASURED] ms
durable 8 KB put with plain fsync, median of 40          [MEASURED] ms
CPython bytes per memtable row (64 MB is [MEASURED] rows)  [MEASURED]
segment bytes per reading, v2, every column              [MEASURED]
segment bytes per reading, v1 (float64 values)           [MEASURED]
v2 with values and timestamps only (the doc's table)     [MEASURED]

all 5 checks passed in [MEASURED] s. README.md lists the test behind each line.
```

## How it works

- The store (archive/store.py, [MEASURED] lines). Object storage is the only truth, so the store copies S3's contract on one local directory: a put writes a staging file, syncs it, installs it with os.link for If-None-Match or with os.replace under a flock for If-Match, then syncs the directory, so the bytes and the name are durable before put returns. A reader sees the whole old object or the whole new one, and an etag is the sha256 of the content. Sync is F_FULLFSYNC on macOS, because plain fsync there stops at the drive's cache. A process frozen inside the flock would stall other processes' If-Match puts. S3 has no such lock, and no test freezes a process.
- The write path and group commit (archive/archive.py, [MEASURED] lines). HTTP threads only validate, quantise and queue a batch. Every 200 ms the commit thread puts every queued batch as one WAL object at the next dense number with If-None-Match, re-reads the manifest to confirm its fencing token is still the newest, applies the rows to the memtable, and only then answers 200. One stream at that window keeps a node at about 5 WAL puts a second, the doc's figure. An unacknowledged batch can still become durable and appear in history, and its ingest_seq reflects when it became durable, not when the client resent it. A writer that cannot tell whether its WAL put landed, or hits any error it does not expect, stops and answers 503 with the reason until it is restarted. Its renewer stops too, so the lease lapses within 10 s and any node can recover. When flushes keep failing, the memtable fills and every ingest gets 429 until a flush succeeds.
- The single writer. After recovery, only the commit thread writes WAL objects and segments. Two threads put the manifest: the commit thread (segments and watermark) and the renewer (lease fields), and the CAS loop in manifest.commit orders them. Recovery runs in the constructor before either thread starts. One writer, one lease, one manifest and one WAL stream cover all 16 shards, because 16 streams at 200 ms would be about 80 puts a second.
- The data model (archive/rules.py, [MEASURED] lines). Facts are never edited. A duplicate keeps its first ingest_seq, the highest (revision, ingest_seq) wins, and every distinct version stays. Keeping the first copy is what makes a late retry a real no-op: (rev 0, A) arrives, then (rev 0, B), then a client resends (rev 0, A). If the resend counted as a new copy, A would come back. So a same-revision change back to an earlier value is ignored, and a duplicate's 200 returns a first_ingest_seq that history never shows. Duplicates are stored, because there is no read before a write, and every read and every compaction collapses them.
- Segments (archive/codec.py, [MEASURED] lines). A flush writes one immutable segment per (shard, day) partition. Values are whole steps of the channel's resolution end to end, and a v2 header stores each channel's step. A value that is not a whole number of steps is refused with 422 instead of rounded, and a segment whose header step differs from today's table is refused, not rescaled. Each station is one zlib stream of delta, delta-of-delta, zigzag and varint columns, followed by an index and a footer whose CRC covers header and index. A block relies on zlib's Adler-32, which catches torn writes in blocks of a few KB. CRC32C per block would be the production choice. Inside a block rows sort by channel and then event_time, so the doc's (station, event_time) order holds inside each channel column.
- The manifest (archive/manifest.py, [MEASURED] lines). manifest.json lists the live segments, the WAL watermark, the next ingest_seq and the lease. Every change is a conditional put in a CAS loop that re-reads, checks the token, re-applies its change to the fresh copy and retries on a conflict, so a commit never erases a concurrent renewal. A commit counter means its content and etag never repeat. It is one JSON object rewritten on every flush, compaction and renewal, and with 16 shards it gains about 5,840 day partitions a year before late data. The answer is a manifest per shard group per month.
- Lease and fencing. The lease is three manifest fields, so one conditional put protects leadership and data together. It is live while now < lease_until, another node may take it at exactly lease_until, and every acquire raises the fencing token. The token is in every WAL header and segment key, every manifest commit checks it, and the writer re-reads it after every WAL put. A new writer also puts an empty fence object at the next free WAL number. A replaced writer that has not noticed yet serves reads from its old view, and answers 503 from the moment it notices, at its next commit or renewal. Renewal uses the local clock, so a clock that jumps backwards can cause an early takeover, never a lost write. Whether recovery lists the WAL before or after taking the lease does not matter, because the fence loop replays anything that lands after the listing. It still takes the lease first, because that is what makes the old writer's re-check fail.
- Recovery. A new writer takes the lease, rebuilds the similarity totals from every live segment, lists the WAL above the watermark, refuses any gap, CRC-checks and replays each object with its stored ingest_seq and ingest_time, then writes its fence. Objects at or below the watermark are never replayed and reads collapse identical rows, so replay is idempotent. One damaged WAL object or segment stops recovery and names the object, where a real system would quarantine that partition and keep serving the rest. The fence loop alone would replay a tail with no gaps, so the WAL listing exists for the gap check, which is the part I would teach. A flush puts its segments, then commits the manifest, then clears the memtable in the same swap, then deletes WAL objects at or below the committed watermark, so a crash at any point loses nothing and an orphan segment is never read.
- History. A read takes one manifest snapshot and a copy of the memtable under one lock, then fetches the footer, header and index and only that station's block from each segment with ranged reads, and merges by revision. One request covers at most 7 days of one station.
- Similar stations (archive/similarity.py, [MEASURED] lines). A station's vector is 60 two-hour means (12 per channel) and 4 quarter-of-day shares of its readings, built only from hourly counts and sums, z-scored across stations and ranked by brute-force cosine in numpy. A flush adds the new day summary minus the old one to the station's totals, so a correction never rescans the other days. Only per-station totals stay in RAM, 20,000 x 1,920 bytes or about 38 MB, where per-day summaries would be 3.5 GB. Vectors cover flushed data only.
- Format versions. Every WAL object and segment carries its version. Readers read segment versions 1 and 2, writers write 2, and compaction rewrites a partition in the current version, which upgrades v1 lazily. v1 stores float64 values, the format I would have shipped before the experiment, so the migration test and the compression lesson tell one story.
- How S3 or GCS maps onto the store (the HTTP API is archive/server.py, [MEASURED] lines). put with if_none_match becomes PutObject with If-None-Match: *, put with if_match becomes PutObject with If-Match, and S3's 412 maps to PreconditionFailed. Under concurrent conditional writes S3 can also answer 409, which the manifest CAS loop would retry and a WAL put would treat like a timeout, stopping the writer so recovery decides. get, get_range, list and delete map to GetObject, GetObject with Range, ListObjectsV2 and DeleteObject, and etags are already opaque. The fencing argument needs strong read-after-write on the manifest and strong list-after-write on the WAL prefix, which S3 has given since December 2020 and GCS gives too. On GCS, ifGenerationMatch plays If-Match and generation 0 means create-only. I did not build or run an adapter, so this is reasoning from the documentation.

## What each test proves

Tests are in tests/, [MEASURED] lines with the harness (doubles.py, conftest.py, workload.py, scenarios.py) and demo.py is [MEASURED] lines. Mutant numbers are in brackets.

| Doc claim | Test | It fails if |
|---|---|---|
| s2 a batch is acknowledged only after its write-ahead object is written | test_ingest_returns_only_after_its_wal_object_is_durable | the ack comes before the put [1] |
| s2 killing any node at any moment loses nothing acknowledged | test_acknowledged_readings_survive_sigkill, test_a_failed_wal_put_stops_the_writer_and_recovery_keeps_every_ack, test_a_flush_interrupted_at_each_step_loses_nothing | replay applies nothing [24], a failed WAL put skips its number [19], the flush order is wrong [6, 7] |
| s2 group commit: several batches share one WAL put | test_acknowledged_readings_survive_sigkill | every batch gets its own put [25] |
| s2 recovery replays write-ahead objects newer than the manifest (edited) | test_a_flush_interrupted_at_each_step_loses_nothing, test_damaged_objects_are_refused_not_skipped | the object at the watermark is replayed [29], a CRC or a gap is ignored [14, 15, 21] |
| s2 duplicates are normal and handled in the data model | test_acknowledged_readings_survive_sigkill (forced resends), test_history_matches_a_dict_oracle_through_flush_compaction_and_reopen | a duplicate keeps its last copy [11] |
| s3 a duplicate is a no-op, even after an overwrite (A, B, A) | test_history_matches_a_dict_oracle_through_flush_compaction_and_reopen | a duplicate keeps its last copy [11] |
| s3 late or out of order needs nothing special | test_history_matches_a_dict_oracle_through_flush_compaction_and_reopen | a read depends on arrival order [12] |
| s3 highest revision wins, ties by ingest_seq, older versions stay | test_history_matches_a_dict_oracle_through_flush_compaction_and_reopen | ties go by value [12], compaction drops versions [13] |
| s3 event time and ingest time are both stored | test_segments_round_trip_and_v2_values_are_much_smaller_than_v1, test_a_flush_interrupted_at_each_step_loses_nothing (reopens under other clocks) | replay stamps its own time [30] |
| s4 memory table capped, 429 instead of growing | test_a_full_memtable_answers_429_at_the_cap_and_not_before | the cap is off by one [18] |
| s4 sorted columnar segments per shard and day | test_segments_round_trip_and_v2_values_are_much_smaller_than_v1 | encoding loses a row |
| s4 manifest replaced by a conditional put | test_a_renewal_between_read_and_put_forces_a_retry_that_keeps_both_changes | a retry re-puts a stale copy [9] |
| s4 one writer through a lease with a fencing token in the manifest | test_another_node_gets_the_lease_exactly_at_the_deadline_and_not_before | the deadline is off by one [10], the token is not checked [8] |
| s4 a paused node that lost its lease cannot commit (edited) | test_a_writer_replaced_mid_put_never_acknowledges_and_loses_nothing | no re-check [2], no fence [3], a collision is not replayed [4], seqs restart from the manifest [5] |
| s4 only that station's byte range through a per-segment index | test_a_history_read_fetches_only_the_stations_byte_range | the reader fetches the whole object [34] |
| s4 merge with the unflushed write-ahead tail by revision | test_history_matches_a_dict_oracle_through_flush_compaction_and_reopen | history ignores the memtable [35] |
| s5 64-number vector, z-scored, cosine, brute force (edited) | test_similar_returns_stations_from_the_same_planted_group | no z-scoring [22] |
| s5 a correction updates the vector by difference | test_a_correction_updates_the_vector_exactly_like_a_full_rebuild | the old day is not subtracted [23] |
| s6 readers understand N and N-1, writers write N, compaction upgrades lazily | test_v1_segments_are_served_beside_v2_and_compaction_rewrites_them_as_v2, test_damaged_objects_are_refused_not_skipped[segment_version_3] | v1 is refused [32], compaction keeps v1 [33], any version is accepted [16] |
| s7 integers at the sensor step, delta, zigzag, varint beat floats | test_segments_round_trip_and_v2_values_are_much_smaller_than_v1 | values are stored without deltas [26] |
| s7 the resolution is stored per channel per segment | test_damaged_objects_are_refused_not_skipped[segment_step_changed] | the header step is ignored [17] |
| s7 conditional writes (If-Match, If-None-Match) | test_the_store_honours_conditions_and_lists_complete_objects_in_order | If-Match is not compared [27], list shows staging files [28] |
| not in the doc: an ambiguous or unexpected error stops the writer | test_a_failed_wal_put_stops_the_writer_and_recovery_keeps_every_ack, test_an_unexpected_error_on_the_commit_thread_stops_the_writer_and_names_it | the writer keeps going [19], the commit thread dies silently [31] |

The kill run is decisive by construction, not by luck. The kill server flushes at 1,500 rows, one commit group holds at most 8 x 60 = 480 rows, and the client whose acknowledgement takes the ledger past 2,000 rows freezes the server with SIGSTOP while it holds the ledger lock. So the first flush has committed and the second cannot have, at least one acknowledged batch lives only in the WAL tail, and requests are still outstanding when SIGKILL lands. The test checks each of these counts and fails the run if any is zero, with no re-roll.

The other tests recover through a fresh Archive object over the persisted store, not a fresh process. The library has no module-level state, so this runs the real recovery path, and the kill test is the one that recovers in a real new process.

Each mutant below is one deliberate mistake in a copy of the repo, run against its target by a script outside the repo. It counts as caught only if the target fails with the expected exception, never with a crash or a timeout. [MEASURED: how many mutants needed their string corrected or a test strengthened, never weakened.]

```
[MEASURED: wave 3 pastes the runner's rows here, unedited, with the date]
 #  change                                                          target                      result
 1  archive._commit: the WAL put becomes a no-op                    test 2                      [MEASURED]
 2  archive._commit: drop the manifest re-check after the WAL put   test 10 after_list, after_gc [MEASURED]
 3  archive._recover: skip the fence put                            test 10 after_list          [MEASURED]
 4  archive._fence: a collision raises Fenced instead of replaying  test 10 after_list          [MEASURED]
 5  archive._replay: next_ingest_seq ignores the replayed headers   test 10 after_list          [MEASURED]
 6  archive._flush: wal_watermark = next_wal_seq instead of applied test 5 wal_delete           [MEASURED]
 7  archive._flush: delete WAL through applied before the commit    test 5 manifest_put         [MEASURED]
 8  manifest.commit: skip the token check                           test 8                      [MEASURED]
 9  manifest.commit: a retry re-puts the copy from before the clash test 9                      [MEASURED]
10  manifest.acquire: now < lease_until becomes now <= lease_until  test 8                      [MEASURED]
11  rules: a duplicate keeps its last copy                          test 11                     [MEASURED]
12  rules.latest: ties broken by value                              test 11                     [MEASURED]
13  compact_rows: rules.latest instead of rules.distinct_versions   test 11                     [MEASURED]
14  codec.decode_wal: skip the CRC check                            test 6 wal_byte_flipped     [MEASURED]
15  archive._recover: skip the gap check                            test 6 wal_object_missing   [MEASURED]
16  codec.Segment: accept any version                               test 6 segment_version_3    [MEASURED]
17  codec.Segment: skip the step check                              test 6 segment_step_changed [MEASURED]
18  archive.ingest: admission uses >=                               test 3                      [MEASURED]
19  archive._commit: a failed WAL put moves to the next number      test 4 not_landed           [MEASURED]
20  codec: a zlib error in a block becomes an empty block           test 6 segment_block_byte_flipped [MEASURED]
21  archive._recover: the gap check compares neighbours only        test 6 wal_first_tail_object_missing [MEASURED]
22  similarity.top_k: no z-scoring                                  test 15                     [MEASURED]
23  archive._refresh_summaries: add new without subtracting old     test 16                     [MEASURED]
24  archive._replay: apply no rows but still advance the counters   test 17                     [MEASURED]
25  archive._commit_loop: take one queued batch per group           test 17                     [MEASURED]
26  codec: v2 values stored without deltas                          test 12                     [MEASURED]
27  store.put: skip the If-Match comparison                         test 1                      [MEASURED]
28  store.list: keep names that start with "."                      test 1                      [MEASURED]
29  archive._recover: the tail filter n > watermark becomes >=      test 5 wal_delete           [MEASURED]
30  archive._replay: ingest_time becomes int(now())                 test 5 segment_put          [MEASURED]
31  archive._commit_loop: remove the catch-all handler              test 7                      [MEASURED]
32  codec: SEGMENT_READ_VERSIONS = (2,)                             test 14                     [MEASURED]
33  archive._compact: write the lowest input version                test 14                     [MEASURED]
34  codec.Segment.rows: fetch the whole object and slice the block  test 13                     [MEASURED]
35  archive.history: leave out the memtable rows                    test 11                     [MEASURED]
```

I did not weaken a test to make the real code pass.

## What is not proven or not built

Not proven:
- Power loss. F_FULLFSYNC and the directory sync are called, but SIGKILL only ends the process and the kernel's page cache survives it, so the kill test proves ordering and recovery, not that the sync works.
- Real S3 or GCS: conditional writes, 409 handling, consistency and latency. Only my local stand-in was tested (doc edit (c)).
- Scale: 20,000 stations, a full 64 MB memtable, a shard-day of 9 million readings, 1 GB of RAM under load. Only peak RSS at 24 stations is measured.
- Compaction memory. The merge decodes one station's block per input at a time, but the compressed output is buffered for the single put, and no test measures either (doc edit (e)).
- Similarity at scale. Totals are rebuilt by scanning every segment at startup, and every flush re-reads the touched station-days, which is quadratic over a day at 20,000 stations. The design answer is a per-station summary stored next to each segment and merged at compaction.
- Memory under many large requests. The 429 check runs after pydantic parses a body, so up to 40 bodies of 10,000 readings can sit in memory at once. uvicorn's limit_concurrency is the setting that bounds it.
- Argued, not tested: safety under clock skew (it rests on tokens, not clocks), the renewer keeping the lease alive through a long compaction, GC never deleting above the committed watermark, the watermark never decreasing, and the flush swap being atomic for a reader running inside it.
- Whether these 64 numbers capture "behaves like" on real stations. Only planted synthetic groups were tried.
- Also untested: rollback to N-1, a second WAL format (with one reader such a test would be vacuous), a new optional channel that defaults to null, read latency and throughput, acknowledgements under 10 ms, the correction storm I am least sure of, CRCs against tampering (they catch torn or damaged bytes only), and reads from a node that is not the writer.

Built differently from the doc:
- One writer, lease, manifest and WAL stream for all 16 shards, which keeps a node at about 5 WAL puts a second where 16 streams would be about 80 (optional doc edit (f)).
- The memtable cap is counted in rows, 64 MB divided by the measured [MEASURED] bytes per row (optional doc edit (d)).
- The vector is 60 two-hour means plus 4 quarter-of-day shares (doc edit (b)). I kept the summary to counts and sums. Variance needs a sum of squares and trend a sum of t times x, both mergeable, and 24-hour autocorrelation spans two days, so it does not fit a per-day summary.
- There is no time-based flush. At low ingest rates vectors can lag past the doc's hour, and the WAL tail and its replay time grow until the next flush.
- Compaction runs on the commit thread, so acknowledgements wait while it runs. The design answer is a merge worker off the acknowledgement path that commits through the same CAS loop.
- Not built at all: deleting compacted segments, orphans and staging files (safe deletion needs a reader grace period), the S3 adapter, the local SSD cache, as-of queries, correction rule records, the quorum log, versioned vector artifacts with as_of, the 90-day window, int8 quantisation, an ANN index, multipart upload, a manifest split per shard group or month, and a supervisor that restarts a stopped writer.

## What I learned

- [WAVE 3: keep this item only after mutant 2 is caught by both cases of test 10.] The marker that makes a stale write-ahead put fail is later garbage collected, and S3 and GCS conditional writes cover one object, so a write-ahead put cannot be made conditional on the lease. Every writer must re-read the manifest after each write-ahead put (doc edit (a)).
- Plain fsync on macOS does not flush the drive's cache, and F_FULLFSYNC does. A durable 8 KB put took a median of [MEASURED] ms with F_FULLFSYNC and [MEASURED] ms with plain fsync. A SIGKILL test cannot tell them apart, because the page cache survives a killed process.
- A memtable row costs about [MEASURED] bytes as a CPython tuple and 41 bytes packed. Python can pack too, with struct.pack_into, array or a numpy structured array, and I kept tuples for clarity. The stronger case for Go or Rust is CPU: pure-Python varints at 0.09 us an int are the hot spot.
- In a real segment the ingest_seq column costs more than the values. v2 with every column is [MEASURED] bytes a reading and values and timestamps alone are [MEASURED], so the doc's 0.24 bytes a point is a values-only figure.
- A duplicate must keep its first ingest_seq, or a late retry brings back an overwritten value (the A, B, A case).
