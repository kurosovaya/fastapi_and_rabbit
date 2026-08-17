FROM python:3.13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

FROM python:3.13-slim
COPY --from=builder /opt/venv /opt/venv
WORKDIR /app
ENV PATH="/opt/venv/bin:$PATH"
# EXPOSE 9001
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "9000"]
