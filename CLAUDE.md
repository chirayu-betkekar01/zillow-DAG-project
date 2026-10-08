# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single Apache Airflow DAG, [zillow_DAG.py](zillow_DAG.py) (`zillow_analytics_DAG`), that pulls Seattle real-estate listings from the Zillow RapidAPI endpoint and loads them into Amazon Redshift via S3. There is no build system, package manifest, linter, or test suite, and the directory is not a git repository. The code runs on an EC2 Ubuntu host that has Airflow installed; the hardcoded `/home/ubuntu/...` paths assume that host, so the DAG does not import on a dev machine.

`DAG.png` is a screenshot of the Airflow graph view for a successful run. `airflow-project-keypair.pem` is the EC2 SSH private key: do not read, print, copy, or commit it.

## Running and testing

Everything runs through the Airflow CLI on the EC2 host, after the DAG file is placed in its `dags/` folder (this repo has no deploy script). Standard Airflow commands:

- `airflow dags test zillow_analytics_DAG 2025-04-30`: run the whole DAG once, without the scheduler.
- `airflow tasks test zillow_analytics_DAG tsk_extract_zillow_data_var 2025-04-30`: run a single task. Task IDs are listed below.

## Pipeline architecture

Four tasks run linearly (`extract >> load_to_s3 >> is_file_in_s3_available >> transfer_s3_to_redshift`), scheduled `@daily` with `catchup=False`:

1. **`tsk_extract_zillow_data_var`** (PythonOperator): calls the RapidAPI `zillow56` `/search` endpoint for `location=seattle, wa`, writes the raw JSON to `/home/ubuntu/response_data_<timestamp>.json`, and returns `[json_path, csv_filename]` through XCom. `csv_filename` is `response_data_<timestamp>.csv`, the name the cleaned file is expected to have later.
2. **`tsk_load_to_s3`** (BashOperator): `aws s3 mv` of the JSON from the EC2 host into the raw bucket `zillow-data-bucket-chirayu`. It uses the AWS CLI's own credentials (instance role or `aws configure`), not the Airflow `aws_s3_conn` connection.
3. **`tsk_is_file_in_s3_available`** (S3KeySensor): waits for the CSV named by XCom `[1]` to appear in the cleaned bucket `zillow-cleaned-csv-data-bucket`.
4. **`tsk_transfer_s3_to_redshift`** (S3ToRedshiftOperator): `COPY`s that CSV into `PUBLIC.zillowdata` with `csv IGNOREHEADER 1`.

**The JSON→CSV transform is not in this repo.** Nothing here writes to `zillow-cleaned-csv-data-bucket`. A separate component (likely an S3-triggered Lambda, though that is not confirmed anywhere in the repo) must read the raw JSON and write a CSV with the matching timestamped name. The sensor only has `timeout=60, poke_interval=5`, so that step must finish within about a minute of the upload or the DAG fails.

## Things to know before editing

- **The timestamp is the filename contract.** `dt_now_string` is computed at module level, at DAG parse time, not per run. It is passed to the extract task and then handed to every downstream task through XCom. Keep downstream tasks reading `ti.xcom_pull("tsk_extract_zillow_data_var")[0|1]` rather than recomputing the name. If you change the filename format, the external transform must change to match.
- **XCom return order matters.** Index `[0]` is the local JSON path (used by `tsk_load_to_s3`) and `[1]` is the CSV filename (used by the sensor and the Redshift load). Reorder or extend the list only together with all three consumers.
- **Parse-time dependency on `/home/ubuntu/airflow/config_api.json`.** The file is read at import and its contents are passed as the HTTP `headers` for the RapidAPI request, so it holds the RapidAPI key and host headers. If the file is missing the whole DAG fails to import, not just the extract task. Keep this file out of the repo.
- **External prerequisites that the code assumes:** Airflow connections `aws_s3_conn` and `conn_id_redshift`, and an existing Redshift table `PUBLIC.zillowdata` whose columns match the cleaned CSV. S3ToRedshiftOperator does not create the table.
- **Airflow 2.x APIs:** `schedule_interval` and `airflow.operators.bash_operator` (the import path is deprecated in favour of `airflow.operators.bash`) are the style currently in use, so match them unless upgrading deliberately.
