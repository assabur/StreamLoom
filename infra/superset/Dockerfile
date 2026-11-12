FROM apache/superset:6.1.0

USER root
RUN uv pip install --python /app/.venv/bin/python --no-cache \
    'clickhouse-connect>=1.8.0,<2.0' 'psycopg2-binary>=2.9,<3.0'
COPY superset_config.py /app/pythonpath/superset_config.py
COPY init.sh bootstrap.py provision.py /app/streamloom/
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
USER superset
