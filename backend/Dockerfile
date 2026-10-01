FROM python:3.13.12-slim@sha256:8bc60ca09afaa8ea0d6d1220bde073bacfedd66a4bf8129cbdc8ef0e16c8a952

WORKDIR /app

# Install system dependencies and poetry in one optimized layer
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get install -y --no-install-recommends \
    curl \
    gcc \
    g++ \
    unixodbc \
    unixodbc-dev \
    exiftool \
    && rm -rf /var/lib/apt/lists/* \
    && apt-get clean \
    && pip install --no-cache-dir "poetry==2.3.2"

# Configure poetry for Docker environment
RUN poetry config virtualenvs.create false \
    && poetry config virtualenvs.in-project false \
    && poetry config cache-dir /opt/poetry-cache

# Copy poetry files first for better layer caching
COPY pyproject.toml poetry.lock ./

# Install dependencies with cache mount
RUN --mount=type=cache,target=/opt/poetry-cache \
    poetry install --only=main --no-interaction --no-ansi --no-root

# Optional local OCR models: pinned upstream artifacts, verified by BuildKit.
# Inference never downloads weights. Enable explicitly with LOCAL_OCR_TESSDATA_PATH.
ADD --chmod=644 --checksum=sha256:7d4322bd2a7749724879683fc3912cb542f19906c83bcc1a52132556427170b2 https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/87416418657359cb625c412a48b6e1d6d41c29bd/eng.traineddata /usr/share/tessdata/eng.traineddata
ADD --chmod=644 --checksum=sha256:4c73ffc59d497c186b19d1e90f5d721d678ea6b2e277b719bee4e2af12271825 https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/87416418657359cb625c412a48b6e1d6d41c29bd/hin.traineddata /usr/share/tessdata/hin.traineddata

ADD --chmod=644 --checksum=sha256:cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30 https://raw.githubusercontent.com/tesseract-ocr/tessdata_fast/87416418657359cb625c412a48b6e1d6d41c29bd/LICENSE /usr/share/tessdata/LICENSE

# Copy entrypoint script with proper permissions
COPY --chmod=755 entrypoint.sh /app/entrypoint.sh

# Copy application code
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/root/.local/bin:$PATH"

ENTRYPOINT ["/bin/bash", "/app/entrypoint.sh"]
