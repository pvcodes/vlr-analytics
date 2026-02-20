FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1
ENV UV_LINK_MODE=copy
ENV UV_NO_DEV=1
ENV UV_TOOL_BIN_DIR=/usr/local/bin

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-editable

COPY main.py ./
COPY scrapers_async/ ./scrapers_async/
COPY scrapers/ ./scrapers/
COPY utils/ ./utils/
COPY data/bronze/events_status.csv ./data/bronze/events_status.csv

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

CMD ["uv", "run", "main.py"]
