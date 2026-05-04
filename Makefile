.PHONY: build release test clippy fmt ci clean install

CARGO  := cargo
TARGET := target/release

BINS := tunasync tunasynctl

# ── Build ──────────────────────────────────────────────────

build:
	$(CARGO) build

release:
	$(CARGO) build --release

# ── Test / Lint ────────────────────────────────────────────

test:
	$(CARGO) test --workspace

clippy:
	$(CARGO) clippy --workspace -- -D warnings

fmt:
	$(CARGO) fmt --all -- --check

fmt-fix:
	$(CARGO) fmt --all

ci: fmt clippy test

# ── Install ────────────────────────────────────────────────

install: release
	install -m 755 $(TARGET)/tunasync   /usr/local/bin/
	install -m 755 $(TARGET)/tunasynctl /usr/local/bin/

# ── Migrate tool (not in release tarball) ──────────────────

migrate:
	$(CARGO) build --release -p tunasync-migrate

install-migrate: migrate
	install -m 755 $(TARGET)/tunasync-migrate /usr/local/bin/

# ── Clean ──────────────────────────────────────────────────

clean:
	$(CARGO) clean