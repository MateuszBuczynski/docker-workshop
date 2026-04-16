FROM python:3.13.11-slim
# Copy uv binary from official uv image (multi-stage build pattern)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

RUN pip install pandas pyarrow

WORKDIR /code
ENV PATH="/code/.venv/bin:$PATH"

# Copy dependency files first (better layer caching)
COPY "pyproject.toml" "uv.lock" ".python-version" ./
RUN uv sync --locked


COPY ingest_data.py .
ENTRYPOINT ["uv","run","python", "ingest_data.py"]


uv run python ingest_data.py \
  --pg-user=root \
  --pg-pass=root \
  --pg-host=pgdatabase \
  --pg-port=5432 \
  --pg-db=ny_taxi \
  --network=pg-network \
  --target-table=yellow_taxi_trips