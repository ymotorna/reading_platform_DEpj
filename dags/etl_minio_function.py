from airflow.models import Variable
import duckdb
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

duckdb_path = "/usr/local/airflow/include/reading_platform.db"

def etl_minio(tbl, col_as_watermark):
    # get watermark for tbl as Airflow Variables
    watermark_id = f'watermark_{tbl}'
    last_loaded_changes = Variable.get(watermark_id, default_var='2000-01-01')
    logger.info("watermark: %s", last_loaded_changes)

    # ------------------------------------------------------
    # connect to  DuckDB
    conn_db = duckdb.connect(duckdb_path)
    # install+load DuckDB extension for MiniO
    conn_db.execute("INSTALL httpfs;")
    conn_db.execute("LOAD httpfs;")

    # get Airflow Variables for credentials
    minio_access_key_id = Variable.get("minio_access_key_id")
    minio_secret_access_key = Variable.get("minio_secret_access_key")
    minio_endpoint = Variable.get("minio_endpoint")
    # Set MinIO configuration \\ code from https://stackoverflow.com/questions/78905256/read-parquet-function-of-duckdb-from-minio-issue
    conn_db.execute(f"SET s3_access_key_id = '{minio_access_key_id}'")
    conn_db.execute(f"SET s3_secret_access_key = '{minio_secret_access_key}'")
    conn_db.execute(f"SET s3_endpoint = '{minio_endpoint}'")
    conn_db.execute("SET s3_use_ssl = false")  # Use true if MinIO uses HTTPS
    conn_db.execute("SET s3_url_style = 'path'")

    # -------------------------------------------------------------
    try:
        conn_db.execute("create schema if not exists raw")
        # get payments
        if tbl == 'payments':
            conn_db.execute("""
                        create table if not exists raw.payments (
                            payment_id VARCHAR PRIMARY KEY,
                            subscription_id VARCHAR,
                            user_id VARCHAR,
                            paid_at TIMESTAMP,
                            amount DOUBLE PRECISION,
                            currency VARCHAR,
                            payment_method VARCHAR,
                            status VARCHAR,
                            billing_period_start DATE,
                            billing_period_end DATE,
                            last_change_at TIMESTAMP
                        )
                        """)

            conn_db.execute(f"""
                            insert into raw.payments
                            select  payment_id ,
                                subscription_id,
                                user_id,
                                paid_at::TIMESTAMP,
                                amount::DOUBLE,
                                currency,
                                payment_method,
                                status,
                                billing_period_start::DATE,
                                billing_period_end::DATE,
                                last_change_at::TIMESTAMP
                            from read_csv_auto('s3://my-reading-platform/payments.csv')
                            where last_change_at::timestamp > '{last_loaded_changes}'
                            on conflict (payment_id) do nothing;
                            """)

        elif tbl == 'reading_sessions':
             conn_db.execute("""
                            create table if not exists raw.reading_sessions (
                                session_id VARCHAR PRIMARY KEY,
                                user_id VARCHAR,
                                book_id VARCHAR,
                                started_at TIMESTAMP,
                                ended_at TIMESTAMP,
                                pages_read INTEGER,
                                device_type VARCHAR,
                                completion_pct DOUBLE PRECISION
                            )
                            """)

             conn_db.execute(f"""
                                insert into raw.reading_sessions
                                select 
                                    session_id,
                                    user_id ,
                                    book_id ,
                                    started_at::TIMESTAMP,
                                    ended_at::TIMESTAMP,
                                    pages_read::INTEGER,
                                    device_type,
                                    completion_pct::DOUBLE
                                from read_json_auto('s3://my-reading-platform/reading_sessions.json')
                                where ended_at::timestamp > '{last_loaded_changes}'
                                on conflict (session_id) do nothing;
                                """)

        elif tbl == 'reviews':
             conn_db.execute("""
                            create table if not exists raw.reviews (
                                review_id VARCHAR PRIMARY KEY,
                                user_id VARCHAR,
                                book_id VARCHAR,
                                rating INTEGER,
                                review_text VARCHAR,
                                created_at TIMESTAMP,
                                is_verified_reader BOOLEAN
                            )
                            """)

             conn_db.execute(f"""
                                insert into raw.reviews
                                select 
                                    review_id,
                                    user_id,
                                    book_id,
                                    rating::INTEGER,
                                    review_text,
                                    created_at::TIMESTAMP,
                                    is_verified_reader::BOOLEAN
                                from read_json_auto('s3://my-reading-platform/reviews.json')
                                where created_at::timestamp > '{last_loaded_changes}'
                                on conflict (review_id) do nothing;
                                """)

        logger.info(f"Inserted/updated rows into raw.{tbl} in DuckDB")
        # set new watermark
        new_watermark = datetime.now().date().isoformat()
        Variable.set(watermark_id, new_watermark)
        logger.info("Watermark set to %s", new_watermark)

    except Exception as e:
        logger.error("Insert into DuckDB failed: %s", e)
        raise  # Airflow raise failed for task
    finally:
        conn_db.close()









