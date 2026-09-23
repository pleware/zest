/// Model download flow — the reusable core shared by the CLI (`zest pull`) and
/// the HTTP API (`POST /v1/pull`). Lists a repo's files, downloads + reconstructs
/// them through the swarm (cache → P2P → CDN), writes the HF cache ref, and
/// returns the snapshot dir.
const std = @import("std");
const Io = std.Io;
const xet = @import("xet");
const config = @import("config.zig");
const swarm = @import("swarm.zig");
const storage = @import("storage.zig");
const bt_peer_mod = @import("bt_peer.zig");
const xet_bridge_mod = @import("xet_bridge.zig");
const parallel_dl = @import("parallel_download.zig");
const ready_mod = @import("ready.zig");
const pull_state = @import("pull_state.zig");

pub const PullResult = struct {
    snapshot_dir: []u8, // owned by the caller — the HF cache snapshot dir
    files_downloaded: usize,
};

/// The JSON body of POST /v1/pull. `repo` is org/name; `revision` defaults to
/// "main"; `file` (optional) restricts the pull to one file (GGUF include);
/// `digest` (optional) is the BLAKE3 hex the file must match.
pub const PullRequest = struct {
    repo: []const u8,
    revision: ?[]const u8 = null,
    file: ?[]const u8 = null,
    digest: ?[]const u8 = null,
};

/// One pin a caller is willing to see a local file become. The ready key IS
/// repo+revision+file, and the digest is what makes an adoption a verification
/// instead of a promise, so all four are required.
pub const AdoptCandidate = struct {
    repo: []const u8,
    revision: []const u8,
    file: []const u8,
    digest: []const u8,
};

/// A request to adopt a file zest did not fetch (POST /v1/adopt).
///
/// The candidates are every pin the caller is prepared to see this file become.
/// The file is hashed once and each digest is a compare against it, because the
/// question an operator actually has is "which pin is this 20 GiB file?" — and
/// answering it one pin at a time would hash the same bytes again per guess.
pub const AdoptRequest = struct {
    path: []const u8,
    candidates: []const AdoptCandidate,
};

/// The verdict on a candidate file. `adopted` counts the pins the file became —
/// every candidate whose digest equals the file's, since one set of weights may
/// legitimately be recorded under more than one pin. Zero is a refusal, and it is
/// the whole answer for a file that is not any of them.
pub const AdoptOutcome = struct {
    /// BLAKE3 of the file at `path`, owned by the caller.
    actual_hex: []u8,
    adopted: usize,
};

/// Download a model repo into the HF cache and return the snapshot dir.
/// Progress goes to `stdout`, warnings/errors to `stderr` (the CLI passes the
/// real streams; the HTTP handler can pass discard/buffer writers). `state`,
/// when non-null, receives byte progress and honours its cancel flag — it lets
/// the async HTTP pull (POST /v1/pull) report progress and be cancelled.
pub fn pullModel(
    allocator: std.mem.Allocator,
    io: Io,
    environ: std.process.Environ,
    cfg: *const config.Config,
    repo_id: []const u8,
    revision: []const u8,
    include_file: ?[]const u8,
    expected_digest: ?[]const u8,
    tracker_url: ?[]const u8,
    enable_p2p: bool,
    direct_peers: []const []const u8,
    stdout: *Io.Writer,
    stderr: *Io.Writer,
    state: ?*pull_state.PullState,
) !PullResult {
    // Step 1: List files from HF Hub via zig-xet
    var file_list = xet.model_download.listFiles(
        allocator,
        io,
        environ,
        repo_id,
        "model",
        revision,
        cfg.hf_token,
    ) catch |err| {
        try stderr.print("Error listing files: {}\n", .{err});
        return err;
    };
    defer file_list.deinit();

    // Resolve revision to an actual commit SHA (e.g. "main" → "607a30d7…")
    const resolved_sha = resolveCommitSha(allocator, io, repo_id, revision, cfg.hf_token);
    defer if (resolved_sha) |s| allocator.free(s);
    const commit: []const u8 = resolved_sha orelse revision;

    try stdout.print("Found {d} files (revision: {s})\n", .{ file_list.files.len, revision });

    // Count Xet-backed files
    var xet_count: usize = 0;
    for (file_list.files) |file| {
        if (file.xet_hash != null) xet_count += 1;
    }

    // Step 2: Initialize swarm downloader (BT-compliant P2P)
    var downloader = try swarm.SwarmDownloader.init(allocator, io, cfg, tracker_url, enable_p2p);
    defer downloader.deinit();

    for (direct_peers) |peer_str| {
        const addr = bt_peer_mod.parseAddress(peer_str) catch {
            try stderr.print("Warning: invalid peer address: {s}\n", .{peer_str});
            continue;
        };
        downloader.addDirectPeer(addr) catch {};
    }

    // Step 3: Initialize XET bridge (cache → P2P → CDN pipeline)
    var bridge = xet_bridge_mod.XetBridge.init(allocator, io, cfg, environ, &downloader);
    defer bridge.deinit();

    if (xet_count > 0) {
        if (cfg.hf_token) |hf_token| {
            bridge.authenticate(repo_id, "model", revision, hf_token) catch |err| {
                try stderr.print("Warning: Xet auth failed ({}), falling back to direct download\n", .{err});
            };
        }
    }

    // Parallel downloader (Io.Group for concurrent xorb fetches)
    var par_dl = parallel_dl.ParallelDownloader.init(
        allocator,
        io,
        &bridge,
        config.default_max_concurrent_downloads,
    );

    // Step 4: Download and reconstruct each file
    var files_done: usize = 0;
    for (file_list.files) |file| {
        // GGUF include: pull only the pinned file, skip the rest of the repo.
        if (include_file) |want| {
            if (!std.mem.eql(u8, want, file.path)) continue;
        }
        files_done += 1;
        try stdout.print("[{d}/{d}] {s}", .{ files_done, file_list.files.len, file.path });

        const output_path = try buildOutputPath(allocator, cfg, repo_id, commit, file.path);
        defer allocator.free(output_path);

        // Already downloaded?
        if (Io.Dir.accessAbsolute(io, output_path, .{})) |_| {
            try stdout.print(" (cached)\n", .{});
            continue;
        } else |_| {}

        if (file.xet_hash) |xet_hash_hex| {
            try stdout.print(" [xet]\n", .{});
            try stdout.flush();

            if (bridge.cas != null) {
                par_dl.reconstructToFile(xet_hash_hex, output_path, state) catch |err| {
                    try stderr.print("  Parallel download error ({}), falling back to sequential\n", .{err});
                    bridge.reconstructToFile(xet_hash_hex, output_path, state) catch |err2| {
                        try stderr.print("  Bridge error ({}), falling back to direct download\n", .{err2});
                        try ensureParentDirs(io, output_path);
                        const dl_config = xet.model_download.DownloadConfig{
                            .repo_id = repo_id,
                            .revision = revision,
                            .file_hash_hex = xet_hash_hex,
                            .hf_token = cfg.hf_token,
                        };
                        xet.model_download.downloadModelToFile(
                            allocator,
                            io,
                            environ,
                            dl_config,
                            output_path,
                        ) catch |err3| {
                            try stderr.print("  Error downloading via xet: {}\n", .{err3});
                            continue;
                        };
                    };
                };
            } else {
                try ensureParentDirs(io, output_path);
                const dl_config = xet.model_download.DownloadConfig{
                    .repo_id = repo_id,
                    .revision = revision,
                    .file_hash_hex = xet_hash_hex,
                    .hf_token = cfg.hf_token,
                };
                xet.model_download.downloadModelToFile(
                    allocator,
                    io,
                    environ,
                    dl_config,
                    output_path,
                ) catch |err| {
                    try stderr.print("  Error downloading via xet: {}\n", .{err});
                    continue;
                };
            }
        } else {
            try stdout.print(" [regular]\n", .{});
            try stdout.flush();
            downloadRegularFile(allocator, io, repo_id, revision, file.path, output_path, state) catch |err| {
                try stderr.print("  Error downloading: {}\n", .{err});
                continue;
            };
        }
    }

    // Verify the BLAKE3 digest of the pinned file (reject on mismatch)
    if (include_file) |file_name| {
        if (expected_digest) |digest_hex| {
            const file_path = try buildOutputPath(allocator, cfg, repo_id, commit, file_name);
            defer allocator.free(file_path);
            const actual_hex = try computeBlake3Hex(allocator, io, file_path);
            defer allocator.free(actual_hex);
            if (!std.mem.eql(u8, actual_hex, digest_hex)) {
                try stderr.print("digest mismatch: expected {s}, got {s}\n", .{ digest_hex, actual_hex });
                Io.Dir.deleteFileAbsolute(io, file_path) catch {};
                return error.DigestMismatch;
            }
            try stdout.print("digest verified: {s}\n", .{digest_hex});
            try ready_mod.markReady(allocator, io, cfg, repo_id, commit, file_name, digest_hex);
        }
    }

    // Write refs file so from_pretrained() resolves
    storage.writeRef(allocator, cfg, repo_id, revision, commit) catch |err| {
        try stderr.print("Warning: failed to write ref: {}\n", .{err});
    };

    const snapshot_dir = try cfg.modelSnapshotDir(repo_id, commit);
    return .{ .snapshot_dir = snapshot_dir, .files_downloaded = files_done };
}

/// Build the output path for a file in the HF cache layout.
fn buildOutputPath(
    allocator: std.mem.Allocator,
    cfg: *const config.Config,
    repo_id: []const u8,
    commit: []const u8,
    file_path: []const u8,
) ![]u8 {
    const snapshot_dir = try cfg.modelSnapshotDir(repo_id, commit);
    defer allocator.free(snapshot_dir);
    return std.fmt.allocPrint(allocator, "{s}/{s}", .{ snapshot_dir, file_path });
}

/// Ensure all parent directories in a path exist.
fn ensureParentDirs(io: Io, path: []const u8) !void {
    if (std.mem.lastIndexOfScalar(u8, path, '/')) |sep| {
        try storage.ensureDirRecursive(io, path[0..sep]);
    }
}

/// Resolve a revision (branch name like "main") to an actual commit SHA by
/// querying the HF API: GET /api/models/{repo}/revision/{revision}.
/// Returns the SHA string, or null if resolution fails (falls back to revision).
fn resolveCommitSha(allocator: std.mem.Allocator, io: Io, repo_id: []const u8, revision: []const u8, token: ?[]const u8) ?[]u8 {
    const url = std.fmt.allocPrint(
        allocator,
        "{s}/api/models/{s}/revision/{s}",
        .{ config.hf_hub_url, repo_id, revision },
    ) catch return null;
    defer allocator.free(url);

    var http_client: std.http.Client = .{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    var aw: Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    var auth_buf: [256]u8 = undefined;
    const auth_header: ?[]const u8 = if (token) |t|
        std.fmt.bufPrint(&auth_buf, "Bearer {s}", .{t}) catch null
    else
        null;

    const extra_headers: []const std.http.Header = if (auth_header) |auth|
        &.{.{ .name = "authorization", .value = auth }}
    else
        &.{};

    const result = http_client.fetch(.{
        .location = .{ .url = url },
        .response_writer = &aw.writer,
        .extra_headers = extra_headers,
    }) catch return null;

    if (result.status != .ok) return null;

    return extractJsonSha(allocator, aw.written());
}

/// Extract the "sha" value from a JSON response.
/// Looks for "sha":"<40-char hex>" pattern.
fn extractJsonSha(allocator: std.mem.Allocator, json: []const u8) ?[]u8 {
    const needle = "\"sha\":\"";
    const pos = std.mem.indexOf(u8, json, needle) orelse return null;
    const start = pos + needle.len;
    if (start + 40 > json.len) return null;

    const sha = json[start..][0..40];
    for (sha) |c| {
        if (!std.ascii.isHex(c)) return null;
    }
    return allocator.dupe(u8, sha) catch null;
}

fn downloadRegularFile(
    allocator: std.mem.Allocator,
    io: Io,
    repo_id: []const u8,
    revision: []const u8,
    file_path: []const u8,
    output_path: []const u8,
    state: ?*pull_state.PullState,
) !void {
    const url = try std.fmt.allocPrint(
        allocator,
        "{s}/{s}/resolve/{s}/{s}",
        .{ config.hf_hub_url, repo_id, revision, file_path },
    );
    defer allocator.free(url);

    var http_client: std.http.Client = .{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    var aw: Io.Writer.Allocating = .init(allocator);
    defer aw.deinit();

    const result = http_client.fetch(.{
        .location = .{ .url = url },
        .response_writer = &aw.writer,
    }) catch return error.HttpError;

    if (result.status != .ok) {
        return error.HttpError;
    }

    try ensureParentDirs(io, output_path);
    try storage.writeFileAtomicAlloc(allocator, io, output_path, aw.written());
    if (state) |s| s.addBytes(aw.written().len);
}

/// Adopt a file zest did not fetch: hash it, and enter the ready registry ONLY
/// when its BLAKE3 equals the pin's digest.
///
/// This is the second way into `ready` (draft 62). The invariant does not move —
/// nothing is served that zest has not verified — but the bytes need not have
/// travelled through zest: a box that already holds the pinned file (llama-swap's
/// `-hf` pull, a reinstall over an existing volume, an artifact copied in by hand)
/// can say so rather than download tens of gigabytes it already has.
///
/// `path` is resolved and must stay inside the models root — the parent of the HF
/// cache. The API is reachable from the host and carries no auth (62 §exposure),
/// so hashing a caller's arbitrary path would let anyone who can reach the port
/// ask "what is the digest of /etc/shadow?".
pub fn adoptLocalFile(
    allocator: std.mem.Allocator,
    io: Io,
    cfg: *const config.Config,
    candidates: []const AdoptCandidate,
    path: []const u8,
) !AdoptOutcome {
    if (candidates.len == 0) return error.NoCandidates;
    for (candidates) |c| {
        if (c.digest.len == 0) return error.MissingDigest;
    }

    const resolved = try std.fs.path.resolve(allocator, &.{path});
    defer allocator.free(resolved);
    const root = std.fs.path.dirname(cfg.hf_cache_dir) orelse cfg.hf_cache_dir;
    if (!insideModelsRoot(resolved, root)) return error.PathOutsideModelsRoot;

    const actual_hex = try computeBlake3Hex(allocator, io, resolved);
    errdefer allocator.free(actual_hex);

    var adopted: usize = 0;
    for (candidates) |c| {
        if (!std.mem.eql(u8, actual_hex, c.digest)) continue;
        try linkIntoHfShelf(allocator, io, cfg, c.repo, c.revision, c.file, resolved);
        try ready_mod.markReady(allocator, io, cfg, c.repo, c.revision, c.file, c.digest);
        adopted += 1;
    }
    return .{ .actual_hex = actual_hex, .adopted = adopted };
}

/// Make the adopted file reachable at the path the generated roster serves from.
///
/// A pull writes the artifact into the HF shelf, and the roster points `-m` at
/// `hf_cache_dir/models--<org>--<repo>/snapshots/<rev>/<file>`
/// (`Config.modelSnapshotDir`). An adopted file lives wherever the operator left
/// it — llama-swap's `-hf` download at the volume root, an artifact copied in by
/// hand — so a ready entry for it would name a path llama-server cannot open, and
/// the box would report a model it cannot load. Link the file into the shelf
/// rather than copying it: the bytes stay where they are, and there stays one
/// layout for every consumer to read (HF's own, symlinks and all).
///
/// Deliberately called even when the registry already holds the pin: the entry is
/// the claim, the link is what makes it loadable, and re-adopting an old entry is
/// how a box repairs itself after this fix.
fn linkIntoHfShelf(
    allocator: std.mem.Allocator,
    io: Io,
    cfg: *const config.Config,
    repo: []const u8,
    revision: []const u8,
    file: []const u8,
    source: []const u8,
) !void {
    const dir = try cfg.modelSnapshotDir(repo, revision);
    defer allocator.free(dir);
    const link_path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir, file });
    defer allocator.free(link_path);

    // Already materialized — by a pull, or by an earlier adoption. Whatever is
    // there is the pin's bytes (a pull verifies before writing), so leave it.
    Io.Dir.accessAbsolute(io, link_path, .{}) catch {
        try storage.ensureDirRecursive(io, dir);
        try Io.Dir.symLinkAbsolute(io, source, link_path, .{});
    };
}

/// Is `path` at or below `root`? A bare prefix test would accept `/models-evil`
/// for the root `/models`, so the byte after the prefix must be a separator.
fn insideModelsRoot(path: []const u8, root: []const u8) bool {
    if (!std.mem.startsWith(u8, path, root)) return false;
    if (path.len == root.len) return true;
    return path[root.len] == '/';
}

/// A scratch box: a models root under /tmp — which is what the path guard
/// measures against — and a ready registry beside it. Each test gets its own
/// root, and the registry file is removed first, so one test's entries cannot
/// leak into another's count when the suite is re-run.
fn adoptTestConfig(cfg: *config.Config, root: []const u8) !void {
    std.testing.allocator.free(cfg.hf_cache_dir);
    cfg.hf_cache_dir = try std.fmt.allocPrint(std.testing.allocator, "{s}/hf", .{root});
    std.testing.allocator.free(cfg.ready_path);
    cfg.ready_path = try std.fmt.allocPrint(std.testing.allocator, "{s}/ready.json", .{root});
    Io.Dir.deleteFileAbsolute(std.testing.io, cfg.ready_path) catch {};
}

test "adoptLocalFile records a file whose BLAKE3 is the pin's digest" {
    var cfg = try config.Config.init(std.testing.allocator, std.testing.io, std.testing.environ);
    defer cfg.deinit();
    const root = "/tmp/zest_adopt_match";
    try adoptTestConfig(&cfg, root);

    const path = root ++ "/weights.gguf";
    try storage.writeFileAtomic(std.testing.io, path, "the pinned bytes");
    const digest = try computeBlake3Hex(std.testing.allocator, std.testing.io, path);
    defer std.testing.allocator.free(digest);

    // A ready entry has to be loadable: the roster points `-m` at the HF shelf, so
    // the adopted file must be reachable exactly there — as a link to where the
    // bytes already are, not a second copy of them. The link is removed first, so
    // this test is about what the adopt call does and not about leftovers from an
    // earlier run in the same /tmp path.
    const shelf = try std.fmt.allocPrint(std.testing.allocator, "{s}/hf/models--org--name/snapshots/abc123/weights.gguf", .{root});
    defer std.testing.allocator.free(shelf);
    Io.Dir.deleteFileAbsolute(std.testing.io, shelf) catch {};

    const pins = [_]AdoptCandidate{.{ .repo = "org/name", .revision = "abc123", .file = "weights.gguf", .digest = digest }};
    const outcome = try adoptLocalFile(std.testing.allocator, std.testing.io, &cfg, &pins, path);
    defer std.testing.allocator.free(outcome.actual_hex);
    try std.testing.expectEqual(@as(usize, 1), outcome.adopted);

    try Io.Dir.accessAbsolute(std.testing.io, shelf, .{});
    var link_buf: [512]u8 = undefined;
    const link_len = try Io.Dir.readLinkAbsolute(std.testing.io, shelf, &link_buf);
    try std.testing.expectEqualStrings(path, link_buf[0..link_len]);

    var parsed = try ready_mod.list(std.testing.allocator, std.testing.io, &cfg);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 1), parsed.value.len);
    try std.testing.expectEqualStrings("org/name", parsed.value[0].repo);
    try std.testing.expectEqualStrings("abc123", parsed.value[0].revision);
    try std.testing.expectEqualStrings("weights.gguf", parsed.value[0].file);
    try std.testing.expectEqualStrings(digest, parsed.value[0].digest);
}

test "adoptLocalFile refuses a file that is not the pinned one" {
    var cfg = try config.Config.init(std.testing.allocator, std.testing.io, std.testing.environ);
    defer cfg.deinit();
    const root = "/tmp/zest_adopt_mismatch";
    try adoptTestConfig(&cfg, root);

    const path = root ++ "/other.gguf";
    try storage.writeFileAtomic(std.testing.io, path, "different bytes");

    // The caller offers the pin it *thinks* this file is; the hash decides.
    const pins = [_]AdoptCandidate{.{ .repo = "org/name", .revision = "abc123", .file = "weights.gguf", .digest = "00" }};
    const outcome = try adoptLocalFile(std.testing.allocator, std.testing.io, &cfg, &pins, path);
    defer std.testing.allocator.free(outcome.actual_hex);
    try std.testing.expectEqual(@as(usize, 0), outcome.adopted);

    // The verdict is the whole answer: a refusal writes nothing.
    var parsed = try ready_mod.list(std.testing.allocator, std.testing.io, &cfg);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed.value.len);
}

test "adoptLocalFile records every pin a file's digest answers to" {
    var cfg = try config.Config.init(std.testing.allocator, std.testing.io, std.testing.environ);
    defer cfg.deinit();
    const root = "/tmp/zest_adopt_two_pins";
    try adoptTestConfig(&cfg, root);

    const path = root ++ "/shared.gguf";
    try storage.writeFileAtomic(std.testing.io, path, "one set of weights, two names");
    const digest = try computeBlake3Hex(std.testing.allocator, std.testing.io, path);
    defer std.testing.allocator.free(digest);

    // One artifact may legitimately be two pins — the same weights recorded under
    // two names. A file hashed once has to be able to satisfy both, or an operator
    // has to guess which name to adopt it as.
    const pins = [_]AdoptCandidate{
        .{ .repo = "org/name", .revision = "rev1", .file = "shared.gguf", .digest = digest },
        .{ .repo = "org/name", .revision = "rev1", .file = "shared-alias.gguf", .digest = digest },
        .{ .repo = "other/thing", .revision = "rev2", .file = "not-this.gguf", .digest = "00" },
    };
    const outcome = try adoptLocalFile(std.testing.allocator, std.testing.io, &cfg, &pins, path);
    defer std.testing.allocator.free(outcome.actual_hex);
    try std.testing.expectEqual(@as(usize, 2), outcome.adopted);

    var parsed = try ready_mod.list(std.testing.allocator, std.testing.io, &cfg);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed.value.len);
}

test "adoptLocalFile refuses a path outside the models root" {
    var cfg = try config.Config.init(std.testing.allocator, std.testing.io, std.testing.environ);
    defer cfg.deinit();
    try adoptTestConfig(&cfg, "/tmp/zest_adopt_outside");

    const pins = [_]AdoptCandidate{.{ .repo = "org/name", .revision = "abc", .file = "f.gguf", .digest = "00" }};
    try std.testing.expectError(
        error.PathOutsideModelsRoot,
        adoptLocalFile(std.testing.allocator, std.testing.io, &cfg, &pins, "/etc/hostname"),
    );
    // A sibling directory that merely starts with the root's name is not inside it.
    try std.testing.expectError(
        error.PathOutsideModelsRoot,
        adoptLocalFile(std.testing.allocator, std.testing.io, &cfg, &pins, "/tmp/zest_adopt_outside-evil/weights.gguf"),
    );
    // And `..` cannot walk out of the root either.
    try std.testing.expectError(
        error.PathOutsideModelsRoot,
        adoptLocalFile(std.testing.allocator, std.testing.io, &cfg, &pins, "/tmp/zest_adopt_outside/../outside-evil/weights.gguf"),
    );
}

test "adoptLocalFile refuses an empty digest" {
    var cfg = try config.Config.init(std.testing.allocator, std.testing.io, std.testing.environ);
    defer cfg.deinit();
    try adoptTestConfig(&cfg, "/tmp/zest_adopt_nodigest");

    // No digest means nothing to verify against, and an unverified file is not
    // adoptable — that is the one thing adoption may not relax (draft 62).
    const undigested = [_]AdoptCandidate{.{ .repo = "org/name", .revision = "abc", .file = "f.gguf", .digest = "" }};
    try std.testing.expectError(
        error.MissingDigest,
        adoptLocalFile(std.testing.allocator, std.testing.io, &cfg, &undigested, "/tmp/zest_adopt_nodigest/whatever.gguf"),
    );
    // And a request that names no pin is a caller error, not a quiet no-op.
    const none = [_]AdoptCandidate{};
    try std.testing.expectError(
        error.NoCandidates,
        adoptLocalFile(std.testing.allocator, std.testing.io, &cfg, &none, "/tmp/zest_adopt_nodigest/whatever.gguf"),
    );
}

/// Compute the BLAKE3 hex digest of a file (streamed — no full read into memory).
fn computeBlake3Hex(allocator: std.mem.Allocator, io: Io, path: []const u8) ![]u8 {
    const file = try Io.Dir.openFileAbsolute(io, path, .{});
    defer file.close(io);

    var hasher = std.crypto.hash.Blake3.init(.{});
    var rbuf: [4096]u8 = undefined;
    var dbuf: [65536]u8 = undefined;
    var reader = file.reader(io, &rbuf);
    while (true) {
        const n = reader.interface.readSliceShort(&dbuf) catch |err| return err;
        if (n == 0) break;
        hasher.update(dbuf[0..n]);
    }

    var digest: [32]u8 = undefined;
    hasher.final(&digest);
    const hex = storage.hashToHex(digest);
    return allocator.dupe(u8, &hex);
}

test "extractJsonSha parses HF API response" {
    const json =
        \\{"_id":"6340","id":"gpt2","sha":"607a30d783dfa663caf39e06633721c8d4cfcd7e","other":"value"}
    ;
    const sha = extractJsonSha(std.testing.allocator, json);
    defer if (sha) |s| std.testing.allocator.free(s);
    try std.testing.expect(sha != null);
    try std.testing.expectEqualStrings("607a30d783dfa663caf39e06633721c8d4cfcd7e", sha.?);
}

test "extractJsonSha returns null for missing sha" {
    const json =
        \\{"id":"gpt2","name":"GPT-2"}
    ;
    const sha = extractJsonSha(std.testing.allocator, json);
    try std.testing.expect(sha == null);
}

test "extractJsonSha rejects invalid hex" {
    const json =
        \\{"sha":"zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"}
    ;
    const sha = extractJsonSha(std.testing.allocator, json);
    try std.testing.expect(sha == null);
}
