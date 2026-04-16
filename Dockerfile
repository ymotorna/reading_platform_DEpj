FROM quay.io/astronomer/astro-runtime:13.6.0

# + dbt venv
RUN python -m venv dbt_venv && \
    . dbt_venv/bin/activate && \
    pip install --no-cache-dir dbt-core dbt-duckdb "duckdb==1.4.4" && \
    deactivate