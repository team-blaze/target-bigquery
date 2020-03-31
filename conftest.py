import os
import json
import pytest
import time
import subprocess
import logging
from random import choice
from string import ascii_uppercase
from google.cloud import bigquery
from google.auth import default as get_credentials


@pytest.fixture(scope="function")
def setup_bigquery_and_config():
    project_id = os.environ.get("GOOGLE_PROJECT_ID")
    if not project_id:
        _, project_id = get_credentials()
    bigquery_client = bigquery.Client(project=project_id)
    datasets = []
    config_files = []

    def setup(
        validate_records=False, stream_data=False, replication_method="HYBRID",
    ):
        dataset_id = "target_bigquery_test_" + "".join(choice(ascii_uppercase) for i in range(12))
        target_config = {
            "project_id": project_id,
            "dataset_id": dataset_id,
            "validate_records": validate_records,
            "stream_data": stream_data,
            "replication_method": replication_method,
            "disable_collection": True,
        }
        config_filename = f"target-config-{dataset_id}.json"
        config_files.append(config_filename)
        with open(config_filename, "w") as f:
            f.write(json.dumps(target_config))

        datasets.append(bigquery_client.create_dataset(dataset_id))
        return project_id, bigquery_client, config_filename, dataset_id

    yield setup

    for config_file in config_files:
        os.remove(config_file)

    for dataset in datasets:
        bigquery_client.delete_dataset(dataset, delete_contents=True, not_found_ok=False)


@pytest.fixture(scope="function")
def check_bigquery():
    def make_check_bigquery(bigquery_client, tablename, assertion, exception_on_fail=True):
        # Wait for BigQuery to have the results.
        # Refresh the table so that it picks up new schema or else it might not return all results.
        query = f"""SELECT * FROM `{tablename}`"""
        retries = 0
        while True:
            query_job = bigquery_client.query(query)
            data = list(query_job.result())

            if assertion(data):
                return data

            if retries > 5:
                if exception_on_fail:
                    raise Exception("Assertion didn't pass with data in BigQuery: {}".format(data))
                else:
                    logging.error("Assertion didn't pass with data in BigQuery: {}".format(data))
                return False

            retries += 1
            time.sleep(2 ** retries)

    return make_check_bigquery


@pytest.fixture(scope="function")
def do_sync():
    def make_do_sync(tap_file, config_filename):
        tap_sample_ps = subprocess.Popen(("cat", tap_file), stdout=subprocess.PIPE,)

        target_ps = subprocess.Popen(
            ("python", "target_bigquery.py", "-c", config_filename),
            stdin=tap_sample_ps.stdout,
            stdout=subprocess.PIPE,
        )

        lines = []
        with target_ps.stdout as pipe:
            for line in iter(pipe.readline, b""):
                print(line)
                lines.append(str(line))
        target_ps.wait()
        return lines

    return make_do_sync
