/// The ready registry — which models are downloaded AND digest-verified.
///
/// A model is `ready` (draft 62) only after zest has downloaded the pinned file
/// and verified its BLAKE3 against the pin's digest. `pullModel` marks a model
/// ready after a successful verify; `fleet sync` reads this registry through
/// `GET /v1/pull` and lists only ready models in the roster. Persisted as JSON
/// at `cfg.ready_path` so the state survives a restart.
const std = @import("std");
const Io = std.Io;
const config = @import("config.zig");
const storage = @import("storage.zig");

pub const Entry = struct {
    repo: []const u8,
    revision: []const u8,
    file: []const u8,
    digest: []const u8,
};

/// Parse the registry. A missing or empty file parses as an empty list.
pub fn list(allocator: std.mem.Allocator, io: Io, cfg: *const config.Config) !std.json.Parsed([]Entry) {
    const opts = std.json.ParseOptions{ .ignore_unknown_fields = true, .allocate = .alloc_always };
    const content = readFile(allocator, io, cfg.ready_path) catch |err| switch (err) {
        error.FileNotFound => return std.json.parseFromSlice([]Entry, allocator, "[]", opts),
        else => return err,
    };
    defer allocator.free(content);
    if (std.mem.trim(u8, content, &std.ascii.whitespace).len == 0) {
        return std.json.parseFromSlice([]Entry, allocator, "[]", opts);
    }
    return std.json.parseFromSlice([]Entry, allocator, content, opts);
}

/// Record a model as ready. Idempotent — a re-verify of an already-ready model
/// is a no-op (an entry is keyed by repo+revision+file).
pub fn markReady(
    allocator: std.mem.Allocator,
    io: Io,
    cfg: *const config.Config,
    repo: []const u8,
    revision: []const u8,
    file: []const u8,
    digest: []const u8,
) !void {
    var parsed = try list(allocator, io, cfg);
    defer parsed.deinit();

    for (parsed.value) |e| {
        if (std.mem.eql(u8, e.repo, repo) and
            std.mem.eql(u8, e.revision, revision) and
            std.mem.eql(u8, e.file, file))
        {
            return; // already ready
        }
    }

    var entries: std.ArrayList(Entry) = .empty;
    defer entries.deinit(allocator);
    try entries.appendSlice(allocator, parsed.value);
    try entries.append(allocator, .{ .repo = repo, .revision = revision, .file = file, .digest = digest });

    const json = try std.json.Stringify.valueAlloc(allocator, entries.items, .{});
    defer allocator.free(json);
    try storage.writeFileAtomic(io, cfg.ready_path, json);
}

/// Read a whole file into an owned buffer (the registry is small).
fn readFile(allocator: std.mem.Allocator, io: Io, path: []const u8) ![]u8 {
    const file = try Io.Dir.openFileAbsolute(io, path, .{});
    defer file.close(io);

    const stat = try file.stat(io);
    const data = try allocator.alloc(u8, stat.size);
    errdefer allocator.free(data);

    var buf: [4096]u8 = undefined;
    var reader = file.reader(io, &buf);
    const n = reader.interface.readSliceShort(data) catch |err| {
        allocator.free(data);
        return err;
    };
    return data[0..n];
}

test "markReady records a verified model and list returns it" {
    var cfg = try config.Config.init(std.testing.allocator, std.testing.io, std.testing.environ);
    defer cfg.deinit();

    // Point the registry at a scratch path (the container is ephemeral, so a
    // fixed path cannot collide). cfg.deinit frees the replacement.
    std.testing.allocator.free(cfg.ready_path);
    cfg.ready_path = try std.testing.allocator.dupe(u8, "/tmp/zest_ready_test.json");
    Io.Dir.deleteFileAbsolute(std.testing.io, cfg.ready_path) catch {};

    try markReady(std.testing.allocator, std.testing.io, &cfg, "org/name", "abc123", "model.gguf", "digest1");
    try markReady(std.testing.allocator, std.testing.io, &cfg, "org/name", "abc123", "model.gguf", "digest1"); // idempotent
    try markReady(std.testing.allocator, std.testing.io, &cfg, "org/name", "abc123", "other.gguf", "digest2");

    var parsed = try list(std.testing.allocator, std.testing.io, &cfg);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 2), parsed.value.len);
    try std.testing.expectEqualStrings("org/name", parsed.value[0].repo);
    try std.testing.expectEqualStrings("abc123", parsed.value[0].revision);
    try std.testing.expectEqualStrings("model.gguf", parsed.value[0].file);
    try std.testing.expectEqualStrings("digest1", parsed.value[0].digest);

    Io.Dir.deleteFileAbsolute(std.testing.io, cfg.ready_path) catch {};
}

test "list returns empty for a missing registry" {
    var cfg = try config.Config.init(std.testing.allocator, std.testing.io, std.testing.environ);
    defer cfg.deinit();

    std.testing.allocator.free(cfg.ready_path);
    cfg.ready_path = try std.testing.allocator.dupe(u8, "/tmp/zest_ready_missing_test.json");
    Io.Dir.deleteFileAbsolute(std.testing.io, cfg.ready_path) catch {};

    var parsed = try list(std.testing.allocator, std.testing.io, &cfg);
    defer parsed.deinit();
    try std.testing.expectEqual(@as(usize, 0), parsed.value.len);
}
