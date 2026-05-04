FROM python:3.12-slim

WORKDIR /app

# System deps for Snowflake connector (libssl, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast dependency installation
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy project definition first for layer caching
COPY pyproject.toml ./
RUN uv pip install --system --no-cache -e ".[async]" 2>/dev/null || \
    uv pip install --system --no-cache \
        "streamlit>=1.32.0" \
        "dbt-sl-sdk[async]>=0.13.0" \
        "snowflake-connector-python[pandas]>=3.6.0" \
        "plotly>=5.18.0" \
        "pandas>=2.0.0" \
        "pyarrow>=14.0.0" \
        "python-dateutil>=2.9.0"

# Copy application source
COPY app/ ./app/

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

ENTRYPOINT ["streamlit", "run", "app/streamlit_app.py", \
    "--server.port=8501", \
    "--server.address=0.0.0.0", \
    "--server.headless=true", \
    "--browser.gatherUsageStats=false"]
