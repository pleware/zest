# zest — the box's P2P model puller (pware-os-workspace/drafts/62).
# Built with Zig 0.16.0 into a static x86_64-linux-musl binary, then dropped
# onto a thin alpine runtime (CA certs only — zest speaks HTTPS to HuggingFace).

FROM alpine:3.20 AS build
ARG ZIG_VERSION=0.16.0
RUN apk add --no-cache curl xz ca-certificates \
    && curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-x86_64-linux-${ZIG_VERSION}.tar.xz" -o /tmp/zig.tar.xz \
    && tar -xf /tmp/zig.tar.xz -C /opt \
    && mv "/opt/zig-x86_64-linux-${ZIG_VERSION}" /opt/zig \
    && rm /tmp/zig.tar.xz
WORKDIR /build
COPY . .
RUN /opt/zig/zig build -Doptimize=ReleaseFast -Dtarget=x86_64-linux-musl

FROM alpine:3.20
RUN apk add --no-cache ca-certificates
COPY --from=build /build/zig-out/bin/zest /usr/local/bin/zest
# The shared models volume: zest downloads into it, llama-swap reads from it.
VOLUME /models
ENV HF_HOME=/models/hf ZEST_CACHE_DIR=/models/zest
ENTRYPOINT ["zest"]
CMD ["serve"]
