from datetime import timedelta

import airflow  # type: ignore
from airflow import DAG  # type: ignore
from airflow.decorators import task  # type: ignore
from airflow.utils.dates import days_ago  # type: ignore

import dependencies.pr2.constants as constants
import dependencies.pr2.transformations as transformations
import dependencies.pr2.utils as utils

default_args = {
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(seconds=15),
    'owner': 'airflow'
}

dag = DAG(
    'pr2-pipeline',
    default_args=default_args,
    description='Pipeline for cleaning, transforming and de-identifying Connect data for PR2',
    start_date=days_ago(1)
)

@task()
def check_api_health() -> None:
    """
    Task to verify PR2 processing container is up.
    """
    
    utils.logger.info("Checking pr2-transformation API status")
    try:
        result = utils.check_service_health(constants.PR2_TRANSFORMATION_CLOUD_RUN_URL)
        if result['status'] != 'healthy':
            error_msg = f"API health check failed. Status: {result['status']}"
            utils.logger.error(error_msg)
            raise Exception(error_msg)

        utils.logger.info(f"The API is healthy! Response: {result}")
    except Exception as e:
        utils.logger.error(f"API health check failed: {str(e)}")
        raise

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def clean_columns(mapping: dict) -> None:
    """Clean columns in survey tables, e.g., coalesce redundant columns, rename them with standardized names."""
    try:
        transformations.clean_columns(mapping)
    except Exception as e:
        error_msg = f"Unable to fix loop variables: {e}"
        utils.logger.error(error_msg)
        raise Exception(error_msg) from e
    
@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def clean_rows(mapping: dict) -> None:
    """Clean rows, e.g., make sure binary questions have cids for yes/no"""
    try:
        transformations.clean_rows(mapping)
    except Exception as e:
        error_msg = f"Unable to fix loop variables: {e}"
        utils.logger.error(error_msg)
        raise Exception(error_msg) from e

@task(max_active_tis_per_dag=10, execution_timeout=timedelta(minutes=60))
def merge_table_versions(mapping: dict) -> None:
    """Merge survey table versions, i.e., perform full outer join, coalescing common columns."""
    try:
        transformations.merge_table_versions(mapping)
    except Exception as e:
        error_msg = f"Unable to merge table versions: {e}"
        utils.logger.error(error_msg)
        raise Exception(error_msg) from e

# Define the DAG structure.
with dag:
    check_api_health = check_api_health()
    clean_columns = clean_columns.expand(mapping=constants.TRANSFORM_CONFIG["clean_columns"]["mappings"])
    clean_rows = clean_rows.expand(mapping=constants.TRANSFORM_CONFIG["clean_rows"]["mappings"])
    merge_table_versions = merge_table_versions.expand(mapping=constants.TRANSFORM_CONFIG["merge_table_versions"]["mappings"])

    # Set task dependencies.
    check_api_health >> clean_columns >> clean_rows >> merge_table_versions