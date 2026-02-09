# Start with slim Python 3.13 image
FROM python:3.13.10-slim

# Set working directory
WORKDIR /app

# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Add virtual environment to PATH so we can use installed packages
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ./

# Install dependencies from lock file (ensures reproducible builds)
RUN uv sync --locked 