FROM quay.io/astronomer/astro-runtime:11.3.0

RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-duckdb dbt-core && \
    deactivate