clean:
    cargo clean

fmt:
    cargo fmt --all

lint:
    cargo fmt --all -- --check

build:
    cargo build

build-release:
    cargo build --release

test:
    cargo test

integration-test:
    ./scripts/run-integration-tests.sh

bench:
    cargo bench

profile: build
    sudo dtrace -c './target/debug/synchrotron' -o out.stacks -n 'profile-997 /execname == "synchrotron"/ { @[ustack(100)] = count(); }'

profile-release: build-release
    sudo dtrace -c './target/release/synchrotron' -o out.stacks -n 'profile-997 /execname == "synchrotron"/ { @[ustack(100)] = count(); }'

profile-svg:
    test -d .flamegraph || git clone https://github.com/brendangregg/FlameGraph.git .flamegraph
    .flamegraph/stackcollapse.pl out.stacks | .flamegraph/flamegraph.pl > profile.svg
    scripts/fix-rust-dtrace-symbols.sh
    open profile.svg
