# nvstor-peer internals

How `nvstor-peer` works inside. How to use it: [peer-copy.md](peer-copy.md).

## Protocol in brief

1. **Rails.** The fetcher opens a connection to rail 0 and sends a signed
   `RAILS` request. The answer lists each server rail's bootstrap port and
   GID. Each fetch connection goes to a server rail round-robin, on the
   local NIC on that rail's plane (see
   [Rails, pairing and routes](peer-copy.md#rails-pairing-and-routes)).
2. **Manifest.** The file list under a prefix is served as an object under
   a reserved key (file keys never start with `/`): version 3 under
   `/manifest3:<prefix>`, version 2 under `/manifest:<prefix>`.
   Format (`crates/nvstor-peer/src/manifest.rs`): magic `BRPM`, version
   3, an authority word (bit 0: the modes and mtimes are the tree's own,
   bit 1: the owners are), count, then per entry: path, kind (file or
   directory), mode (`st_mode & 0o7777`), mtime (seconds and
   nanoseconds), uid, gid, and for a file its size, a 128-bit generation
   and a 128-bit content identity. Every regular file and every directory
   below the prefix is listed (the prefix directory itself is not).
   Version 2 is the files only (size, generation and identity), version 1
   without the identity; both are still decoded, and the decoder reports
   which version it read. The decoder refuses absolute paths, `.`, `..`
   and empty components, for directories as for files, so a server cannot
   make a fetcher write outside its destination; it also refuses an
   unknown kind, unknown authority bits, mode bits beyond `0o7777` and
   nanoseconds of a second or more. The fetcher maps the listing onto the
   destination as a tree keyed by (parent, name), so a listing costs its
   own size however deep its paths. The listing is built once and served
   from a cache for its ranges; if it changes while being read, the
   fetcher fetches it again (up to 3 times).
3. **Files.** Every file is split into ranges; one queue feeds all
   workers, so thousands of small files cost no connection setup and a few
   large files still use every worker. Each range is a signed `GET` of
   (path, offset, generation). The server reads the range into a slot of
   its registered pool (O_DIRECT when aligned) and RDMA-WRITEs it into the
   fetcher's registered buffer, then answers with the length, the
   generation and a CRC-32C of exactly those bytes. The fetcher checks the
   CRC and writes the range to disk from the same buffer (O_DIRECT when
   aligned).
4. **Generations.** The server derives a file's generation from `fstat`
   (device, inode, size, mtime, ctime) before and after every read and
   repeats a read during which it changed. A range whose generation no
   longer matches the manifest fails with `GENERATION_CHANGED`; ranges of two
   versions are never stitched together. The fetch finishes the file from
   another source with the same content identity, or, when none is left,
   stops with an error naming the file ([Several sources](#several-sources-fetch---source----source-)).

## Fan-out

What it does and how to use it: [peer-copy.md](peer-copy.md#fan-out-many-fetchers-one-origin).

The problem: a NIC's message rate drops sharply once it holds a few
thousand active connections, because its cache of connection state
thrashes. Every fetch opens one peer per worker plus one more (the
anchor), each with 4 QPs (lanes): 33 peers and 132 QPs on the source for
a default fetch. A few dozen concurrent fetchers would put a source past
that point long before its disks or links are the limit, and a few
hundred replicas starting at once would also hit the 4,096-peer accept
limit.

### The cap: `serve --max-fetchers K`

- A fetcher is a session
  ([peer-copy.md](peer-copy.md#authentication-and-access)), not a connection: one fetch counts once
  however many rails, workers and reconnects it uses.
- A new session is admitted while fewer than K hold a connection; every
  connection of an admitted session is admitted after it.
- Over the cap, the bootstrap answers **BUSY** after checking the
  connector's MAC and before the server creates any QP, CQ or memory
  registration (`nvstor_rdma::bootstrap::SessionAdmitter`, hooked into
  `RdmaTransport::set_session_admitter`). A refused fetcher costs the
  source's NIC no connection state, and learns at once, without a timeout,
  to try another source. A connector without the secret still gets REJECT
  and never learns whether the server is busy.
- A session's place is freed when its last connection is torn down: a
  clean disconnect, or the transport's liveness watcher seeing the
  bootstrap stream close (a killed fetcher, within 200 ms). Each fetch
  keeps one connection to its source open for its whole life (the anchor,
  which also answers the rail lookup), so a fetch never has zero
  connections between reconnects and cannot lose its place mid-copy.

Why K = 4 by default. One fetch usually runs at its destination's write
rate, while a source can send several times that. Model the tree as
generations: every source serves K fetchers, each at the smaller of the
destination's rate and the source's rate divided by K, and when they
finish the number of sources grows (1 + K)-fold. With no cap, every
fetcher shares one source and the copy takes as long as one source
sending every copy. With K = 1 each generation is fast but the tree grows
slowly. In between, the time to N copies is lowest around K = 3 or 4,
where each fetcher still gets close to its destination's rate and the
tree grows four- or fivefold per generation. 4 is chosen over 3 because a
destination slower than the source's share (smaller disks, a busier node)
moves the optimum up, not down. Real fetches do not finish in lockstep,
so the tree grows continuously rather than in generations; the shape of
the trade-off is the same. At K = 4 a source holds 528 QPs for fetchers,
which leaves room for NFS over RDMA and NCCL on the same NIC. This is a
model, not a measurement.

### Several sources: `fetch --source ... --source ...`

- The source order is a hash of each name with the fetcher's seed (from
  `--client-name`, the host name by default): every fetcher has its own
  order, so the first choices spread over the list, and a name keeps its
  place when the list grows.
- A selection round tries every live source in that order. BUSY: next one.
  Connection or setup failure: next one; a source that fails
  `max_retries + 1` rounds in a row (6 in the CLI) is dropped. A source
  whose manifest is missing or malformed is dropped. When a round finds no
  taker, the fetcher sleeps 250 ms doubling to 5 s, with jitter in
  [half, full], and resolves the names again; after `--wait-s` without a
  taker it fails with "no source available". When every source has been
  dropped it fails at once with the reasons.
- Mid-copy failure: a worker that spends its retries on a source (a
  dropped connection that will not come back, repeated CRC errors) takes
  that source out of the running. The next worker to need a connection
  selects a replacement while the others wait; the replacement's manifest
  must list every file not yet complete with the same size and the same
  **content** identity. Its generations replace the old source's for the
  remaining ranges (they are that node's own version tokens and are never
  compared across nodes), and each range is still CRC-checked and pinned
  to a generation on the node that serves it. Files already renamed into
  place stay; a file in progress keeps its finished ranges and continues.
  A source with other content is never read from (`sources disagree on
  the content` when none is left).
  A source whose file ends before the size its own manifest gives served
  inconsistently: it is taken out of the running like one with CRC
  errors, and every file it wrote any bytes of (that file included, and
  files it had already finished) is fetched again whole from the
  replacement; writes still in flight from the old source to those files
  are refused, a finished file is fetched beside itself and replaces
  itself, and the content record follows. Each file is refetched by one
  worker. A finished file is refetched only from a source that lists it
  with the same size and content identity; when none does, the fetch
  ends with `sources disagree on the content` (exit 5).
  Bad data is held against the server, not the connection or the name:
  every name of that server (its IPv4 and IPv6 addresses, recognised by
  its identity as below) is out, no write served by any connection to it
  lands from then on, and every file any of them wrote is refetched. The
  sources still in use get fresh views at once, so the refetches can be
  served from them. A worker still connected to the server that used to
  answer at a revived name does not take the new server out.
- Failover timing. A source that crashes or exits is noticed at once:
  its bootstrap TCP stream closes, the transport removes the peer, and
  every request in flight on it fails. A source that hangs while its TCP
  stream stays open is noticed only when a control reply misses
  `CONTROL_REPLY_TIMEOUT`, 38 s; each such miss removes the connection
  and counts as one failed attempt, so a hung source costs up to
  `max_retries + 1` of them before it is taken out of the running. The
  bound is the server's worst case to answer plus a margin
  (`crates/nvstor-rdma/src/s3_rdma_proto.rs`):

  | Term | Bound | What |
  |---|---|---|
  | `ADMIT_WAIT` | 1 s | dispatcher waits for a worker; then NO_SLOT |
  | `SLOT_WAIT` | 2 s | GET / ALLOC wait for a pool slot; then NO_SLOT |
  | `STORAGE_ALLOWANCE` | 10 s | one storage callback (documented, not enforced) |
  | `LANE_OP_WORST` | 10 s | one RDMA WRITE / READ: credits 5 s + completion 5 s |
  | `LANE_OP_WORST` | 10 s | the reply SEND |
  | **`SERVER_REPLY_WORST`** | **33 s** | sum: longest a healthy server takes to answer |
  | `REPLY_MARGIN` | 5 s | |
  | **`CONTROL_REPLY_TIMEOUT`** | **38 s** | client wait for a reply (`transport.rs` `recv_response`) |

  A busy source is never a failed one. NO_SLOT (no free slot or worker)
  is load on one request, not a broken connection: the worker keeps its
  connection and the fetch keeps its place on that source (its anchor
  is never released for being busy, so a fetcher cap cannot give the
  place away). The worker pauses (decorrelated jitter per worker:
  uniform between `busy_backoff` and three times its last pause, capped
  at `max_busy_backoff`; it starts over after progress) and asks again
  on the same connection, or, if another eligible source is already
  validated in this fetch, or one can be validated (by one worker at a
  time, outside the selection lock), moves that range there while the
  busy source cools. A source's manifest is read once per fetch; a file
  that changes there later fails its ranges as Changed. A BUSY on a new
  connection (fetcher cap) is handled the same way. Busy answers spend
  no retries; the fetch ends with "no range completed for N s"
  (Unavailable) only after `--wait-s` passes without any range
  completing (counted from when the first source was chosen; an idle
  validated source still takes a busy range with `--wait-s 0`; a
  `--wait-s` too large to represent means no bound). Manifest reads wait
  out busy answers on their connection for up to `--wait-s` too, except
  while choosing or adding a source: there a source whose manifest stays
  busy past a short budget (`max_busy_backoff`) cools and the next one is
  tried, so a busy source never holds failover to an idle one. A source
  is checked (manifest read) by one worker at a time and never twice;
  selection, checks and name resolution run outside the shared state
  lock, and a claim on a source is released even if the check panics.
  Workers drop their connections to a source the fetch gave up on at
  their next range boundary.
- A file that changes on one source mid-copy (its generation moves, as a
  `touch`, `chmod` or rewrite does) takes that server out of the running
  (all its names, e.g. its IPv4 and IPv6 addresses, recognised by the
  server's identity: the random instance id its process sends in the
  RAILS reply, or, from a server without one, its rails' GIDs and
  ports) and the range moves to another server, whose content identity
  is checked as for any failover. So if the origin is rewritten
  mid-fetch, the copy is finished from a replica that holds the content
  the fetch started from, and the summary line says so ("files changed
  on ORIGIN during the fetch, finished from other sources with the
  original content"; `FetchReport::changed_on`). When that leaves no
  source at all, the fetch fails with "changed on the source during the
  fetch"; when sources are left but do not finish (busy past
  `--wait-s`), their failure is reported and the change is mentioned.
- A source given up on (dead) stays out for the fetch, unless its address
  later reaches another server (a reused pod address: another instance
  id): its name is checked again every `2 * max_busy_backoff` (a 1 s TCP
  connect first, at most two names per selection round, never past the
  busy bound) and, if the server's identity differs, it is a new
  candidate, at most three times per name. A source that died serving
  corrupt bytes is never tried again. Workers drop connections to the
  old server at that name at their next range.

### Content identity

The manifest (since version 2) carries, next to each file's size and
generation, a 128-bit `content` identity:

- On a node that has the original file, `content` is the file's
  generation there (device, inode, size, mtime, ctime). It names "this
  version of this file on the origin".
- A fetch records, for each file it renamed into place, the identity it
  fetched under and the local copy's generation right after the rename, in
  `DEST/.nvstor-peer-content` (`crates/nvstor-peer/src/content.rs`).
  Serving that directory, the export advertises the recorded identity for
  each file whose generation is still the recorded one, and the file's own
  generation for anything else. A file rewritten after the copy is never
  presented as the origin's bytes.
- So identities pass down the tree unchanged, two copies of one origin
  agree, and two independent copies (say, two nodes that each downloaded
  the weights) disagree even when the bytes are equal. That errs on the
  safe side: a fetch never mixes ranges of two sources unless both vouch
  for the same origin version. The tradeoff: identical bytes from
  unrelated origins are not recognised, which would need a whole-file hash
  on every source, read before serving.
- `fetch` prints `manifest::content_digest` (SHA-256 over path, size and
  content of every file) and puts it in `--ready-file`: equal digests mean
  equal trees from the same origin version. Directories and attributes
  (mode, mtime, owner) are not part of it, nor of the comparison a
  failover makes: a copy made from a version 2 manifest (which lists no
  attributes) has the same digest and identities as one made from
  version 3, and the content record stays in version 2.
- Names starting with `.nvstor-peer-` are nvstor-peer's own (staging,
  the record, a ready file placed in DEST): never listed or served, and a
  fetch refuses to create them.

## GPU memory path

These points describe `bench --gpu`. `fetch --gpu` uses the same
registration and check, with one buffer holding a slot area and the whole
copy ([design/fetch-gpu.md](design/fetch-gpu.md)).

- `nvstor-peer` loads the CUDA driver (`libcuda.so.1`) at run time; it
  links no CUDA and runs unchanged on nodes without a GPU. It allocates
  one buffer (`cuMemAlloc`, one range-sized slice per connection), exports
  it as a dma-buf (`cuMemGetHandleForAddressRange`) and registers that on
  every local NIC (`ibv_reg_dmabuf_mr`, rdma-core 34 or later, resolved at
  run time so hosts with an older rdma-core still run `fetch` and `serve`).
- The CPU cannot always read GPU memory directly (on some platforms a
  load faults and `cuMemSetAccess` refuses host access). So a range that
  landed is checked over a copy: the NIC's writes are first made visible
  to the GPU (`cuFlushGPUDirectRDMAWrites` on a device that does not order
  them itself; a device that can do neither is refused), then copied into
  the worker's pinned host buffer on its own stream and CRC-32C checked,
  all inside the measured operation.
- Security is the same as for host slots: the buffer is registered for
  remote write only, and only servers this client authenticated to are
  connected to it, each writing where a signed GET named.
- `nvstor_rdma::region` is the library interface: `RailRegion::dmabuf`
  registers any dma-buf (for example memory another process exported)
  on every NIC of a `Rails`, and `RdmaSource::fetch_to` lands a range in a
  slice of it and returns it unchecked with the server's CRC-32C
  (`Landed`), for the caller to check where it can read the bytes.

## Attributes

A fetch gives every file and directory it creates the attributes the
source lists (`crates/nvstor-peer/src/fetch.rs` has the full contract).
Unit-tested only (mixed modes, owners, set-id files and directories,
sticky and empty directories, nanosecond mtimes, `--preserve-setid` and
`--no-owner`); not yet run on RDMA hardware.

- **Mode**: permission and sticky bits as listed, hardened: set-user-ID
  and set-group-ID bits are dropped, from files and directories, unless
  `--preserve-setid` and the listed owner was applied to that very file
  (a copy should not hand out privileges or group inheritance on its
  own: kept on a file the copier owns, a 04755 of uid 1000 would become
  set-user-ID root). Without the owner they are dropped and the summary
  says `setid dropped (owner not applied)`; `--preserve-setid` with
  `--no-owner` is refused. The bits are read back after they are set:
  the kernel clears S_ISGID without an error for a process without
  CAP_FSETID whose groups do not include the file's, and the summary
  then says `setid kept but cleared by the kernel on N`. Write for others
  is dropped from a directory without the sticky bit.
- **Mtime**, to the nanosecond. The atime is the time of the copy: the
  manifest does not carry one, and serving a file already reads it.
- **Owner and group**, as numbers (like `rsync --numeric-ids`: no name is
  looked up on either side, so the two nodes must agree on the ids), when
  the fetch has CAP_CHOWN, CAP_DAC_OVERRIDE and CAP_FOWNER in its
  effective set (not merely uid 0: root with its capabilities dropped has
  none) and `--no-owner` is not given. The last two are needed because a
  directory's end record and final mode come after its chown (below).
  This is decided once, up front, from the effective set read with
  capget(2): without all three the files are the caller's, nothing fails
  for it, and the summary names the capability that was missing (`owner
  kept: no CAP_DAC_OVERRIDE`). Likewise, when a uid or gid the source
  lists has no mapping in this process's user namespace
  (`/proc/self/uid_map`, `gid_map`), owners are not applied and the
  summary says `owner kept: ids not mapped in this user namespace`. A
  chown the system refuses (a user namespace without the ids mapped, NFS
  root_squash) ends the fetch at the first file, naming `--no-owner`.
- **Directories**, empty ones included, below `--prefix`. `DEST` itself
  is the operator's and keeps its attributes.

How: staging files are created 0600 and directories 0700, so nothing is
readable by others while the copy runs. A complete file gets, on its open
descriptor and before the rename, `futimens` and `fchmod` (without set-id
bits) while the fetch still owns it, so neither needs CAP_FOWNER; then
`fchown` (it moves ctime, not mtime); then, only when set-id bits are
kept, `fchmod` with them (the chown cleared them; this needs CAP_FOWNER);
then `fsync` (not `fdatasync`, so the attributes are as durable as the
bytes), and only then the rename: a file never appears in place with
other attributes, and if any step fails it does not appear at all.

A directory gets, in order: its owner (while it is still 0700 and the
fetcher's, so the fetcher's group never holds the listed group bits,
even across a crash), its mode with owner rwx added while its end record
is still to be written, the end record, its final mode (set-id bits when
kept), and its mtime last. A crash or a refused chown before the record
leaves the start record: never an end record over a directory that is
still the copier's. A directory whose attributes fail is put back to
0700 with its start record (0770 of the fetcher's would open it to the
fetcher's group) and fsynced. If the start record cannot be renamed
back, the record file is rewritten in place; if even that fails, a
full-authority start record is restored by removing the file, and a
lower one is logged as an error. This is why owners take
CAP_DAC_OVERRIDE and CAP_FOWNER as well as CAP_CHOWN: writing into and
chmod-ing a directory that is no longer the fetcher's. A directory that
fails in the final pass does not stop the others; the fetch then fails
with the first error.

After every file is in place each directory gets its attributes and is
fsynced, deepest first, on a descriptor opened beneath `DEST` without
following symlinks, so no later rename moves a directory's mtime and a
read-only directory (0555) still takes its files. A fetch again over a
copy whose directories are read-only works (the caller's own directories
get owner rwx for the copy), and puts back any attribute changed
locally, since every fetch rewrites every file.

With `--no-attrs` nothing is applied: files stay 0600, created
directories 0700, owned by the caller with the copy time. The caller sets
modes afterwards; a copy is private by default, never world-readable
because of a umask.

### Authority records

A copy only has the attributes its fetch applied. The fetch records that
in `.nvstor-peer-attrs` (`modes=1 owners=0`, written under fresh random
temporary names) in DEST and in every directory of the copy that has
something to say: a directory's record is what the fetch applies AND
every record at or above it, its own earlier one included, so files an
earlier copy left there keep what was said of them; only a directory
this fetch created (everything in it is this fetch's) may end with more
than it started with. Full authority in a directory that had no record
is not written (absence says it), and the end record is not written when
it equals the start one: a copy made with owners has records in no
directory but DEST, one made without has one per directory, written
once. A copy thereby describes itself in every directory. These are
`.nvstor-peer-*` names: never listed, served or copied, and to be left
out of any count of a copy's files (see
[Comparing a copy with its source](peer-copy.md#comparing-a-copy-with-its-source)).

Durability: the start records are written unsynced and then fsynced
together (in parallel) with their directories before any file is
renamed in; the end records are written before each directory's fsync
in the final pass and their contents fsynced together after it. After a
crash a directory's record is therefore its start value, its end value,
or empty/partial (which reads as no authority): never higher than the
final one. `cargo test -p nvstor-peer --release --lib time_2000 --
--ignored --nocapture` times a fetch of a tree with thousands of
directories without owners, the case that writes the most records.

A `serve` puts the authority in its version 3 listing. The rule is
fail-safe: the authority of a listing is the AND of every record found
from the listed directory up to `/` and of every record in the listed
subtree. Only reaching `/` with no record and no error gives full
authority; any error on the way (a directory that cannot be opened or
searched, a record that cannot be read or does not parse) gives none.
`..` is opened with O_PATH, so an ancestor with search but no read
permission (0711) is gone through. A fetch uses the same rule for its
own record: before anything is renamed it writes what it applies AND
what holds over DEST from above; at the end, the same again (and, when
DEST held nothing else when it started, without DEST's old record). A
fetch into a directory of a copy made without owners therefore records
no owners either, and serving any directory of such a copy, or a parent
of it, says so. A listing reads the records above it both before and
after it walks the tree, and serves the AND of both: a fetch that starts
into the tree while it is being listed cannot have its files listed
under the authority from before. A fetch without the destination's lock
(a filesystem without `flock`, such as NFS) cannot rule out another
fetch renaming its files into the same tree: it never takes DEST for
empty, never takes a directory it made for all its own, and so no record
anywhere ends higher than it started. On such filesystems authority can
only stay or go down. NFS mounted with `local_lock=flock` (or `nolock`)
grants `flock` per host only: two fetches on two hosts into the same
DEST both count as locked there. Run one fetch per DEST across hosts.

A record counts only if it is owned by the serving user or root; one of
anyone else's counts as no authority. Records only ever lower authority:
a local user who can create `.nvstor-peer-attrs` somewhere inside an
export (a sticky world-writable directory, say) can take authority away
from what is served (the fetch then keeps its own ownership and private
modes), never add it.

A local user with write access to a directory that already exists in
the destination can plant something at its record's name (a directory
called `.nvstor-peer-attrs`, say): the fetch then cannot write the
record and skips that directory and everything below it, reporting it.
That denies the copy of that directory; it never raises authority.

Authority is per directory. A directory of a copy served on its own (a
bind mount, a Kubernetes subPath) still carries its own record, so it is
served with the copy's authority. Trees not produced by nvstor-peer
(copies made by other tools) have no record in their subdirectories:
served through a bind mount of a subdirectory, the walk cannot see the
record above the bind source, and the subtree is served as
authoritative. Likewise a file moved or hard-linked out of a reduced
copy into a tree without records is served as authoritative there: the
record describes the directory, not the file. When a directory's chown
fails, the fetch puts it back to 0700; if even that fails it logs an
error and says so in the fetch's error. A fetch from a copy made without
owners keeps its own ownership (`owner not authoritative at source`)
instead of giving every file the copier's uid; from a copy made with
`--no-attrs` it applies no mode either. A fan-out tree therefore keeps
attributes end to end when every hop applies them, and says so when one
did not. DEST is taken to hold one fetched tree.

**Failures.** A directory that cannot be created or opened (EACCES, a
name the filesystem refuses), and a path longer than PATH_MAX or with a
name longer than NAME_MAX, are reported per entry with everything below
them; the rest is copied and the fetch exits nonzero (`Incomplete`). When
a fetch fails, the directories it created or opened up still get their
listed attributes (or their old mode back) as far as possible; files
already in place are complete with their attributes; nothing else of the
copy is left.

**Versions and upgrade order.** A source serves two listings: version 3
(attributes, directories) under `/manifest3:<prefix>` and version 2
(files only) under `/manifest:<prefix>`. So:

- a fetcher that reads only version 2 keeps working against a current
  source (bytes only);
- a current fetcher refuses a source that serves no version 3 listing
  with `serves manifest version 2 only ... upgrade the source first`, and
  moves on to another source if it has one; `--no-attrs` takes it, bytes
  only, private modes;
- so upgrade sources (and replicas that serve) first, then fetchers.

After a failover the attributes still come from the listing the copy
started from. A replacement source is asked for the version 2 listing,
which every source serves: it must match sizes and content identities
only, and its own attributes are neither compared nor applied, so the
whole tree gets one listing's attributes. The content identity and digest
do not include attributes.

## Staging and atomicity

- Bytes land in a private staging directory,
  `DEST/.nvstor-peer-staging-<random>` (mode 0700), one file per target
  named by its index. Only a complete, verified file is given its
  attributes, synced and renamed (`renameat`) to its final name;
  directories are given theirs and fsynced at the end. A file that exists
  under its final name is complete.
- The staging directory is removed on success and on every error. A fetch
  that crashed leaves one behind; the next fetch into the same `DEST`
  removes it. The server never lists staging directories.
- A fetch holds `flock(LOCK_EX)` on `DEST`; a second fetch into the same
  directory fails at once ("another nvstor-peer fetch is writing into
  this directory", exit 6). On a filesystem without directory `flock` it
  warns and continues; staging is still private.
- Nothing under `DEST` is resolved through a symlink: directories are
  created and opened with `openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)`
  relative to `DEST`, and `renameat` replaces a name without writing
  through it. An existing entry that is not a regular file where a file
  goes, or not a directory where a directory goes, fails the fetch before
  any transfer.
- On the source, every directory is opened from the root descriptor with
  `openat2(RESOLVE_BENEATH | RESOLVE_NO_SYMLINKS)`, files with
  `RESOLVE_BENEATH`, and only regular files are read. Symlinks, FIFOs and
  devices are skipped (and logged); a file that vanishes during the walk is
  skipped.
- Errors that can be retried (a busy server, a CRC mismatch, a dropped
  connection) are retried with exponential backoff, up to 5 retries per
  range; failed reconnects count against the same budget. Finished ranges
  are kept.
