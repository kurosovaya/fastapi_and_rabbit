FROM python:3.13-slim AS builder
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/
ENV UV_PROJECT_ENVIRONMENT=/opt/venv
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --no-dev

FROM python:3.13-slim
COPY --from=builder /opt/venv /opt/venv
WORKDIR /app
COPY services/shared/ ./shared/
ENV PATH="/opt/venv/bin:$PATH"
EXPOSE 9000
HEALTHCHECK --interval=5s --timeout=3s --retries=5 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:9000/health')" || exit 1
CMD ["sh", "-c", "python shared/declare_queues.py && uvicorn main:app --host 0.0.0.0 --port 9000"]
