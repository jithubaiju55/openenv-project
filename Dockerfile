# ── SQL Repair Environment — Dockerfile ──────────────────────────────────────
#
# Build:   docker build -t sql-repair-env .
# Run:     docker run -p 7860:7860 sql-repair-env
# Health:  curl http://localhost:7860/health
#
# HF Spaces Docker containers MUST listen on port 7860.
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim

# Non-root user for security (required by HF Spaces)
RUN useradd -m -u 1000 appuser

WORKDIR /app

# Install dependencies first (Docker layer cache optimization)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Give ownership to non-root user
RUN chown -R appuser:appuser /app

USER appuser

# Environment variables (override in HF Spaces settings if needed)
ENV PORT=7860
ENV HOST=0.0.0.0
ENV WORKERS=4
ENV MAX_CONCURRENT_ENVS=100
ENV PYTHONPATH=/app

EXPOSE 7860

# Health check — HF Spaces pings this
HEALTHCHECK --interval=30s --timeout=10s --start-period=15s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:7860/health')" || exit 1

CMD ["sh", "-c", "uvicorn server.app:app --host ${HOST} --port ${PORT} --workers ${WORKERS}"]
