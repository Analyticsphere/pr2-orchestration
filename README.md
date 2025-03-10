# pr2-orchestration

This repository holds the Airflow DAG for the PR2 Pipeline and it's dependencies. However, the DAG simply makes POST requests to REST API endpoints that trigger Cloud Runs which ultimately transform the data and read/write to BigQuery. Those Cloud Runs live here: https://github.com/Analyticsphere/pr2-transformation

