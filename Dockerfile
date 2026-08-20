FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TRADERRD_DB_PATH=/var/lib/traderrd/traderrd.sqlite3 \
    TELEGRAM_SESSION_PATH=/var/lib/traderrd/traderrd

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY scripts ./scripts
RUN pip install --no-cache-dir . \
    && groupadd --system traderrd \
    && useradd --system --gid traderrd --home-dir /var/lib/traderrd --create-home traderrd \
    && mkdir -p /var/lib/traderrd \
    && chown -R traderrd:traderrd /var/lib/traderrd /app

USER traderrd
VOLUME ["/var/lib/traderrd"]
ENTRYPOINT ["traderrd"]
CMD ["demo-run", "--no-tui"]
