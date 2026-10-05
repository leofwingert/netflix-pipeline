import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
DATASET_GOLD = os.getenv("GCP_DATASET_GOLD", "netflix_analitical")
bucket_env = os.getenv("GCP_BUCKET_NAME", "")
BUCKET = bucket_env if bucket_env.startswith("gs://") else f"gs://{bucket_env}"

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2024, 1, 1),
}

def run_pyspark_bronze_to_silver():
    import sys
    scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "scripts"))
    for path in ["/opt/airflow/scripts", scripts_path]:
        if path not in sys.path:
            sys.path.append(path)
    
    from spark_bronze_to_silver import main
    main()

with DAG(
    dag_id="netflix_elt_pyspark_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["pyspark", "bigquery", "docker"],
) as dag:

    # Executa a limpeza e transformação Bronze -> Silver com PySpark
    task_pyspark_clean = PythonOperator(
        task_id="pyspark_bronze_to_silver",
        python_callable=run_pyspark_bronze_to_silver,
    )
    sql_create_fact_ratings = f"""
    CREATE OR REPLACE EXTERNAL TABLE `{PROJECT_ID}.{DATASET_GOLD}.ext_silver_ratings`
    OPTIONS (
      format = 'PARQUET',
      uris = ['{BUCKET}/silver/ratings/*.parquet']
    );

    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET_GOLD}.fact_ratings` AS
    SELECT * FROM `{PROJECT_ID}.{DATASET_GOLD}.ext_silver_ratings`;
    """

    task_update_fact_ratings = BigQueryInsertJobOperator(
        task_id="update_fact_ratings_gold",
        configuration={
            "query": {
                "query": sql_create_fact_ratings,
                "useLegacySql": False,
            }
        },
    )

    # Atualiza as Views Analíticas para o Dashboard (Streamlit)
    sql_refresh_views = f"""
    CREATE OR REPLACE VIEW `{PROJECT_ID}.{DATASET_GOLD}.vw_top_movies` AS
    SELECT
      movie_id,
      title,
      genres,
      release_year,
      total_ratings,
      ROUND(avg_rating, 2) AS avg_rating
    FROM `{PROJECT_ID}.{DATASET_GOLD}.vw_movies_kpis`
    WHERE total_ratings >= 20
    ORDER BY avg_rating DESC, total_ratings DESC
    LIMIT 10;
    """

    task_refresh_views = BigQueryInsertJobOperator(
        task_id="refresh_analytical_views",
        configuration={
            "query": {
                "query": sql_refresh_views,
                "useLegacySql": False,
            }
        },
    )

    # Tasks de início e fim da pipeline
    start_task = EmptyOperator(task_id="start_task")
    end_task = EmptyOperator(task_id="end_task")

    # Ordem das dependências no pipeline:
    start_task >> task_pyspark_clean >> task_update_fact_ratings >> task_refresh_views >> end_task