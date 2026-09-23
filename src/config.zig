const std = @import("std");
const Io = std.Io;
const Environ = std.process.Environ;
const peer_id_mod = @import("peer_id.zig");

pub const hf_hub_url = "https://huggingface.co";
pub const default_revision = "main";
pub const default_dht_port: u16 = 6881;
pub const default_listen_port: u16 = 6881;
pub const default_http_port: u16 = 9847;
/// Loopback by default: a standalone zest exposes its API to its own machine
/// and nothing else. The box's compose sets `ZEST_HTTP_HOST=0.0.0.0`, because
/// there the CLI drives `pware backend data` and `fleet sync` from outside the
/// container — and while the listener sits on the container's loopback the
/// published `9847:9847` is a dead forward, so the roster quietly comes out
/// empty (drafts/62).
pub const default_http_host = "127.0.0.1";
pub const default_max_peers: u16 = 50;
pub const default_chunk_target_size: u32 = 65536; // 64KB — matches HF Xet CDC chunk size
pub const default_max_concurrent_downloads: u32 = 16;

/// Well-known BT DHT bootstrap nodes.
pub const dht_bootstrap_nodes = [_]struct { host: []const u8, port: u16 }{
    .{ .host = "router.bittorrent.com", .port = 6881 },
    .{ .host = "dht.transmissionbt.com", .port = 6881 },
};

pub const Config = struct {
    allocator: std.mem.Allocator,
    io: Io,
    hf_token: ?[]const u8,
    cache_dir: []const u8,
    hf_cache_dir: []const u8,
    xorb_cache_dir: []const u8,
    chunk_cache_dir: []const u8,
    peer_id: [20]u8,
    dht_port: u16,
    listen_port: u16,
    http_port: u16,
    /// Owned. What the HTTP API binds, as `host:port` — built once here and
    /// rebuilt as a whole by `setHttpHost` / `setHttpPort`, so the listener
    /// never derives an address from half-updated pieces.
    http_addr: []const u8,
    http_host: []const u8,
    max_peers: u16,
    chunk_target_size: u32,
    pid_file_path: []const u8,
    ready_path: []const u8,

    pub fn init(allocator: std.mem.Allocator, io: Io, environ: Environ) !Config {
        const home_owned = try getEnv(environ, allocator, "HOME");
        defer if (home_owned) |h| allocator.free(h);
        const home = home_owned orelse "/root";

        const hf_cache_dir = (try getEnv(environ, allocator, "HF_HOME")) orelse
            try std.fmt.allocPrint(allocator, "{s}/.cache/huggingface/hub", .{home});

        const cache_dir = (try getEnv(environ, allocator, "ZEST_CACHE_DIR")) orelse
            try std.fmt.allocPrint(allocator, "{s}/.cache/zest", .{home});

        const xorb_cache_dir = try std.fmt.allocPrint(allocator, "{s}/xorbs", .{cache_dir});
        const chunk_cache_dir = try std.fmt.allocPrint(allocator, "{s}/chunks", .{cache_dir});
        const pid_file_path = try std.fmt.allocPrint(allocator, "{s}/zest.pid", .{cache_dir});
        const ready_path = try std.fmt.allocPrint(allocator, "{s}/ready.json", .{cache_dir});

        const hf_token = try readHfToken(allocator, io, environ, home);

        // Parse optional env var overrides
        const http_port_str = try getEnv(environ, allocator, "ZEST_HTTP_PORT");
        defer if (http_port_str) |p| allocator.free(p);
        const http_port = if (http_port_str) |p|
            std.fmt.parseInt(u16, p, 10) catch default_http_port
        else
            default_http_port;

        const http_host = (try getEnv(environ, allocator, "ZEST_HTTP_HOST")) orelse
            try allocator.dupe(u8, default_http_host);
        const http_addr = try std.fmt.allocPrint(allocator, "{s}:{d}", .{ http_host, http_port });

        const max_peers_str = try getEnv(environ, allocator, "ZEST_MAX_PEERS");
        defer if (max_peers_str) |p| allocator.free(p);
        const max_peers = if (max_peers_str) |p|
            std.fmt.parseInt(u16, p, 10) catch default_max_peers
        else
            default_max_peers;

        return .{
            .allocator = allocator,
            .io = io,
            .hf_token = hf_token,
            .cache_dir = cache_dir,
            .hf_cache_dir = hf_cache_dir,
            .xorb_cache_dir = xorb_cache_dir,
            .chunk_cache_dir = chunk_cache_dir,
            .peer_id = peer_id_mod.generate(io),
            .dht_port = default_dht_port,
            .listen_port = default_listen_port,
            .http_port = http_port,
            .http_addr = http_addr,
            .http_host = http_host,
            .max_peers = max_peers,
            .chunk_target_size = default_chunk_target_size,
            .pid_file_path = pid_file_path,
            .ready_path = ready_path,
        };
    }

    pub fn deinit(self: *Config) void {
        if (self.hf_token) |token| self.allocator.free(token);
        self.allocator.free(self.pid_file_path);
        self.allocator.free(self.ready_path);
        self.allocator.free(self.xorb_cache_dir);
        self.allocator.free(self.chunk_cache_dir);
        self.allocator.free(self.cache_dir);
        self.allocator.free(self.hf_cache_dir);
        self.allocator.free(self.http_addr);
        self.allocator.free(self.http_host);
    }

    /// Move the HTTP API to another host — `0.0.0.0` when something outside the
    /// container has to reach it. The address is rebuilt after the new host and
    /// the old pair is freed only once the new one exists, so a failure leaves
    /// the config as it was rather than pointing at a freed string.
    pub fn setHttpHost(self: *Config, host: []const u8) !void {
        const owned = try self.allocator.dupe(u8, host);
        errdefer self.allocator.free(owned);
        const addr = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ owned, self.http_port });
        self.allocator.free(self.http_host);
        self.allocator.free(self.http_addr);
        self.http_host = owned;
        self.http_addr = addr;
    }

    pub fn setHttpPort(self: *Config, port: u16) !void {
        const addr = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ self.http_host, port });
        self.allocator.free(self.http_addr);
        self.http_addr = addr;
        self.http_port = port;
    }

    /// Build the HF cache path for a model snapshot:
    /// ~/.cache/huggingface/hub/models--{org}--{name}/snapshots/{commit}/
    pub fn modelSnapshotDir(self: *const Config, repo_id: []const u8, commit: []const u8) ![]u8 {
        // Replace '/' with '--' in repo_id
        var sanitized: std.ArrayList(u8) = .empty;
        defer sanitized.deinit(self.allocator);
        for (repo_id) |c| {
            if (c == '/') {
                try sanitized.appendSlice(self.allocator, "--");
            } else {
                try sanitized.append(self.allocator, c);
            }
        }
        return std.fmt.allocPrint(
            self.allocator,
            "{s}/models--{s}/snapshots/{s}",
            .{ self.hf_cache_dir, sanitized.items, commit },
        );
    }

    /// Build the xorb cache path: ~/.cache/zest/xorbs/{prefix}/{hash}
    pub fn xorbCachePath(self: *const Config, hash_hex: []const u8) ![]u8 {
        if (hash_hex.len < 4) return error.InvalidHash;
        return std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}/{s}",
            .{ self.xorb_cache_dir, hash_hex[0..2], hash_hex },
        );
    }

    /// Build the chunk cache path: ~/.cache/zest/chunks/{prefix}/{hash}
    pub fn chunkCachePath(self: *const Config, hash_hex: []const u8) ![]u8 {
        if (hash_hex.len < 4) return error.InvalidHash;
        return std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}/{s}",
            .{ self.chunk_cache_dir, hash_hex[0..2], hash_hex },
        );
    }
};

/// Cross-platform env lookup. `getPosix` is POSIX-only — on Windows the environ
/// is a WTF-16 block — so read through `getAlloc`, returning an owned value (or
/// null when the variable is unset).
fn getEnv(environ: Environ, allocator: std.mem.Allocator, key: []const u8) !?[]u8 {
    return environ.getAlloc(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableMissing => null,
        else => |e| e,
    };
}

fn readHfToken(allocator: std.mem.Allocator, io: Io, environ: Environ, home: []const u8) !?[]const u8 {
    // Try HF_TOKEN env var first
    if (try getEnv(environ, allocator, "HF_TOKEN")) |env| {
        return env;
    }

    // Try reading from ~/.cache/huggingface/token
    const token_path = try std.fmt.allocPrint(allocator, "{s}/.cache/huggingface/token", .{home});
    defer allocator.free(token_path);

    const file = Io.Dir.openFileAbsolute(io, token_path, .{}) catch return null;
    defer file.close(io);

    // Read token file (max 4096 bytes)
    var buf: [4096]u8 = undefined;
    var reader = file.reader(io, &.{});
    const n = reader.interface.readSliceShort(&buf) catch return null;
    const content = buf[0..n];

    // Trim whitespace
    const trimmed = std.mem.trim(u8, content, &std.ascii.whitespace);
    return try allocator.dupe(u8, trimmed);
}

test "Config init and deinit" {
    var cfg = try Config.init(std.testing.allocator, std.testing.io, std.testing.environ);
    defer cfg.deinit();
    try std.testing.expect(cfg.cache_dir.len > 0);
    try std.testing.expect(cfg.hf_cache_dir.len > 0);
    try std.testing.expect(cfg.xorb_cache_dir.len > 0);
}

test "the HTTP API address follows the host and the port" {
    var cfg = try Config.init(std.testing.allocator, std.testing.io, std.testing.environ);
    defer cfg.deinit();
    // Loopback unless the environment says otherwise: a standalone zest exposes
    // nothing, and the box opts out with ZEST_HTTP_HOST=0.0.0.0.
    try std.testing.expectEqualStrings("127.0.0.1:9847", cfg.http_addr);
    try cfg.setHttpHost("0.0.0.0");
    try std.testing.expectEqualStrings("0.0.0.0:9847", cfg.http_addr);
    try cfg.setHttpPort(9899);
    try std.testing.expectEqualStrings("0.0.0.0:9899", cfg.http_addr);
}

test "modelSnapshotDir" {
    var cfg = try Config.init(std.testing.allocator, std.testing.io, std.testing.environ);
    defer cfg.deinit();
    const path = try cfg.modelSnapshotDir("meta-llama/Llama-3.1-8B", "abc123");
    defer std.testing.allocator.free(path);
    try std.testing.expect(std.mem.indexOf(u8, path, "models--meta-llama--Llama-3.1-8B") != null);
    try std.testing.expect(std.mem.indexOf(u8, path, "snapshots/abc123") != null);
}

test "xorbCachePath" {
    var cfg = try Config.init(std.testing.allocator, std.testing.io, std.testing.environ);
    defer cfg.deinit();
    const path = try cfg.xorbCachePath("abcdef1234567890");
    defer std.testing.allocator.free(path);
    try std.testing.expect(std.mem.indexOf(u8, path, "/ab/abcdef1234567890") != null);
}
