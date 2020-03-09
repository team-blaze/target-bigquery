import os
from time import sleep
from random import choice
from string import ascii_uppercase
from datetime import datetime, timedelta


random_dataset_id = "target_bigquery_test_" + "".join(choice(ascii_uppercase) for i in range(12))
test_path = os.path.dirname(os.path.realpath(__file__))


def test_hybrid_multiple_runs(setup_bigquery_and_config, check_bigquery, do_sync):
    project_id, bigquery_client, config_filename = setup_bigquery_and_config(random_dataset_id)
    table = f"{project_id}.{random_dataset_id}.fruitimals"

    # This is the beginning of a stream, setting up a new table and populating with several rows
    do_sync(f"{test_path}/tap-sample-first-run.json", config_filename)

    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 7)

    # Some more rows with the same schema
    do_sync(f"{test_path}/tap-sample-incremental-rows.json", config_filename)

    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 12)

    # A new, incompatible schema – normally this would break the sync as BigQuery returns an error
    # when trying to update the table with a schema which makes previous rows invalid
    max_run_time = datetime.now() + timedelta(seconds=300)
    while max_run_time > datetime.now():
        do_sync(f"{test_path}/tap-sample-new-schema.json", config_filename)

        result = check_bigquery(
            bigquery_client, table, lambda data: len(data) == 13, exception_on_fail=False
        )

        if result:
            break

        sleep(10)

    assert {*result[0].keys()} == {"asset", "name", "deleted", "created_at", "updated_at", "id"}

    # No actual rows in this, but the target still needs to correctly run and return the state
    stdout = do_sync(f"{test_path}/tap-sample-nothing-new.json", config_filename)

    assert 'version": 1593427048885' in stdout[0]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 13)
