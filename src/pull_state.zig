/// In-flight pull registry + per-pull progress state.
///
/// A pull runs in a background thread (spawned by POST /v1/pull); this module
/// tracks its byte progress and cancellation so the HTTP API can report it
/// (GET /v1/pull/{id}/progress) and cancel it (POST /v1/pull/{id}/cancel).
///
/// The pull thread writes the byte counters and reads the cancel flag; the HTTP
/// handler reads the counters and writes the cancel flag. The counters + cancel
/// flag are atomic, so no lock is taken on the hot path. The terminal fields
/// (snapshot_dir, error_msg) are written once at the end under a `.release`
/// store on `state`, which the reader observes after an `.acquire` load.
const std = @import("std");
const Io = std.Io;

/// Where a pull is in its lifecycle.
pub const State = enum(u8) {
    running = 0,
    done = 1,
    failed = 2,
    cancelled = 3,
};

/// Per-pull progress + cancellation state, shared between the pull thread and
/// the HTTP handlers. Owned by the Registry, which frees it.
pub const PullState = struct {
    id: []u8, // owned
    repo: []u8, // owned
    revision: []u8, // owned
    file: ?[]u8, // owned
    bytes_done: std.atomic.Value(u64),
    bytes_total: std.atomic.Value(u64),
    state: std.atomic.Value(u8), // State
    cancel: std.atomic.Value(bool),
    snapshot_dir: ?[]u8, // owned; set on done
    error_msg: ?[]u8, // owned; set on failed/cancelled

    pub fn init(
        allocator: std.mem.Allocator,
        id: []const u8,
        repo: []const u8,
        revision: []const u8,
        file: ?[]const u8,
    ) !*PullState {
        const ps = try allocator.create(PullState);
        errdefer allocator.destroy(ps);
        ps.* = .{
            .id = try allocator.dupe(u8, id),
            .repo = try allocator.dupe(u8, repo),
            .revision = try allocator.dupe(u8, revision),
            .file = if (file) |f| try allocator.dupe(u8, f) else null,
            .bytes_done = std.atomic.Value(u64).init(0),
            .bytes_total = std.atomic.Value(u64).init(0),
            .state = std.atomic.Value(u8).init(@intFromEnum(State.running)),
            .cancel = std.atomic.Value(bool).init(false),
            .snapshot_dir = null,
            .error_msg = null,
        };
        return ps;
    }

    pub fn deinit(self: *PullState, allocator: std.mem.Allocator) void {
        allocator.free(self.id);
        allocator.free(self.repo);
        allocator.free(self.revision);
        if (self.file) |f| allocator.free(f);
        if (self.snapshot_dir) |s| allocator.free(s);
        if (self.error_msg) |e| allocator.free(e);
        allocator.destroy(self);
    }

    pub fn getState(self: *PullState) State {
        return @enumFromInt(self.state.load(.acquire));
    }

    pub fn isCancelled(self: *PullState) bool {
        return self.cancel.load(.acquire);
    }

    pub fn requestCancel(self: *PullState) void {
        self.cancel.store(true, .release);
    }

    pub fn addBytes(self: *PullState, n: u64) void {
        _ = self.bytes_done.fetchAdd(n, .monotonic);
    }

    /// Record the terminal state. Write the terminal fields first, then the
    /// `.release` store on `state`, so a reader that `.acquire`-loads a terminal
    /// state sees them.
    pub fn finish(
        self: *PullState,
        allocator: std.mem.Allocator,
        st: State,
        snapshot_dir: ?[]const u8,
        err: ?[]const u8,
    ) void {
        if (snapshot_dir) |s| self.snapshot_dir = allocator.dupe(u8, s) catch null;
        if (err) |e| self.error_msg = allocator.dupe(u8, e) catch null;
        self.state.store(@intFromEnum(st), .release);
    }
};

/// Mutex-protected registry of in-flight pulls, keyed by pull id.
pub const Registry = struct {
    allocator: std.mem.Allocator,
    io: Io,
    mutex: Io.Mutex,
    map: std.StringHashMap(*PullState),
    next_id: u64,

    pub fn init(allocator: std.mem.Allocator, io: Io) Registry {
        return .{
            .allocator = allocator,
            .io = io,
            .mutex = Io.Mutex.init,
            .map = std.StringHashMap(*PullState).init(allocator),
            .next_id = 0,
        };
    }

    pub fn deinit(self: *Registry) void {
        var it = self.map.valueIterator();
        while (it.next()) |ps| ps.*.deinit(self.allocator);
        self.map.deinit();
    }

    /// Register a new pull and return its state. The returned pointer is owned
    /// by the registry — the caller must not free it.
    pub fn start(
        self: *Registry,
        repo: []const u8,
        revision: []const u8,
        file: ?[]const u8,
    ) !*PullState {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        self.next_id += 1;
        const id = try std.fmt.allocPrint(self.allocator, "pull-{d}", .{self.next_id});
        defer self.allocator.free(id);
        const ps = try PullState.init(self.allocator, id, repo, revision, file);
        errdefer ps.deinit(self.allocator);
        try self.map.put(ps.id, ps);
        return ps;
    }

    /// Look up a pull by id. The returned pointer stays valid until the registry
    /// is deinitialised; the caller reads its atomics directly (no lock).
    pub fn get(self: *Registry, id: []const u8) ?*PullState {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        return self.map.get(id);
    }
};

test "PullState lifecycle" {
    const a = std.testing.allocator;
    const ps = try PullState.init(a, "pull-1", "org/repo", "main", "model.gguf");
    defer ps.deinit(a);

    try std.testing.expectEqualStrings("pull-1", ps.id);
    try std.testing.expectEqualStrings("org/repo", ps.repo);
    try std.testing.expectEqual(State.running, ps.getState());
    try std.testing.expect(!ps.isCancelled());

    ps.addBytes(42);
    ps.addBytes(58);
    try std.testing.expectEqual(@as(u64, 100), ps.bytes_done.load(.acquire));

    ps.requestCancel();
    try std.testing.expect(ps.isCancelled());

    ps.finish(a, .done, "/snap", null);
    try std.testing.expectEqual(State.done, ps.getState());
    try std.testing.expectEqualStrings("/snap", ps.snapshot_dir.?);
}

test "Registry start and get" {
    const a = std.testing.allocator;
    var reg = Registry.init(a, std.testing.io);
    defer reg.deinit();

    const ps1 = try reg.start("org/repo", "main", null);
    const ps2 = try reg.start("org/other", "main", "f.gguf");
    try std.testing.expectEqualStrings("pull-1", ps1.id);
    try std.testing.expectEqualStrings("pull-2", ps2.id);

    const got = reg.get("pull-1").?;
    try std.testing.expectEqual(ps1, got);
    try std.testing.expect(reg.get("nope") == null);
}
