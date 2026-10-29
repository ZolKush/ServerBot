FROM python:3.14.7-slim-trixie@sha256:51dafde81dbdb6ebde285137a295cf18a47ca95234fe388a343719cb97305b3d

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    MAINTBOT_CONFIG_DIR=/etc/maintbot/conf \
    SECRETS_ENV_PATH=/run/secrets/maintbot.env

WORKDIR /opt/maintbot
COPY app/requirements.txt app/requirements.txt
RUN python -m pip install --no-cache-dir -r app/requirements.txt \
    && groupadd --gid 10001 maintbot \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin maintbot \
    && install -d -m 0700 -o maintbot -g maintbot /var/lib/maintbot

COPY app/ app/
COPY deploy/docker-entrypoint.sh /usr/local/bin/maintbot-entrypoint
RUN chmod 0555 /usr/local/bin/maintbot-entrypoint

USER 10001:10001
STOPSIGNAL SIGTERM
ENTRYPOINT ["/bin/sh", "/usr/local/bin/maintbot-entrypoint"]
CMD ["python", "-m", "app.launcher"]
