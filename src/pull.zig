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
