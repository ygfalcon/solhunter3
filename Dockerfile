FROM python:3.11-slim

# Install build tools and Rust
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential curl pkg-config libssl-dev && rm -rf /var/lib/apt/lists/*
RUN curl https://sh.rustup.rs -sSf | sh -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Copy source code
WORKDIR /app
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir .

# Build Rust crates
RUN cargo build --manifest-path route_ffi/Cargo.toml --release --features=parallel \
    && cp route_ffi/target/release/libroute_ffi.so solhunter_zero/ \
    && cargo build --manifest-path depth_service/Cargo.toml --release \
    && mkdir -p target/release \
    && cp depth_service/target/release/depth_service target/release/depth_service

# Use the Python entry point directly
ENTRYPOINT ["python", "-m", "solhunter_zero.launcher"]
CMD ["--auto"]
