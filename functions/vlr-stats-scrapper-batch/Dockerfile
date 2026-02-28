# 1) Builder stage — install dependencies
FROM python:3.12-slim AS builder

# Install uv
RUN pip install --no-cache-dir uv

WORKDIR /app

# Copy only the lock and manifest for caching deps
COPY pyproject.toml uv.lock ./

# Install dependencies only (no project files)
RUN uv sync --locked --no-install-project

COPY main.py main.py
COPY scrapers/ scrapers/
COPY utils/ utils/


# Ensure the venv bin is first
ENV PATH="/app/.venv/bin:$PATH"

ENTRYPOINT ["uv", "run", "main.py"]
CMD []