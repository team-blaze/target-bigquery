import os
from random import choice
from string import ascii_uppercase


random_dataset_id = "target_bigquery_test_" + "".join(choice(ascii_uppercase) for i in range(12))
test_path = os.path.dirname(os.path.realpath(__file__))


def test_hybrid_multiple_runs(setup_bigquery_and_config, check_bigquery, do_sync):
    project_id, bigquery_client, config_filename = setup_bigquery_and_config(random_dataset_id)
    table = f"{project_id}.{random_dataset_id}.fruitimals"

    # This is the beginning of a stream, setting up a new table and populating with several rows
    stdout = do_sync(f"{test_path}/tap-sample-first-run.json", config_filename)

    assert 'version": 1573504566181' in stdout[0]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 7)

    # Some more rows with the same schema
    stdout = do_sync(f"{test_path}/tap-sample-incremental-rows.json", config_filename)

    assert 'version": 1574426993906' in stdout[0]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 12)

    # New schema, needs to recreate and repopulate table
    stdout = do_sync(f"{test_path}/tap-sample-new-schema.json", config_filename)

    assert 'version": 1583426993906' in stdout[0]
    assert check_bigquery(
        bigquery_client,
        table,
        lambda data: len(data) == 13
        and {*data[0].keys()} == {"asset", "name", "deleted", "created_at", "updated_at", "id"},
    )

    # No actual rows in this, but the target still needs to correctly run and return the state
    stdout = do_sync(f"{test_path}/tap-sample-nothing-new.json", config_filename)

    assert 'version": 1593427048885' in stdout[0]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 13)

    # Some more rows with the same schema but also a state line midstream
    stdout = do_sync(
        f"{test_path}/tap-sample-incremental-rows-with-state-midstream.json", config_filename
    )

    assert 'version": 1693427999999' in stdout[0]
    assert 'version": 1693429999888' in stdout[1]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 15)
