import os
import simplejson as json
from decimal import Decimal

from target_bigquery import persist_lines_hybrid


test_path = os.path.dirname(os.path.realpath(__file__))


def test_hybrid_multiple_runs(setup_bigquery_and_config, check_bigquery, do_sync):
    project_id, bigquery_client, config_filename, dataset_id = setup_bigquery_and_config()
    table = f"{project_id}.{dataset_id}.fruitimals"

    # This is the beginning of a stream, setting up a new table and populating with several rows
    stdout, _ = do_sync(f"{test_path}/tap-sample-first-run.json", config_filename)

    assert 'version": 1573504566181' in stdout[0]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 7)

    # Some more rows with the same schema
    stdout, _ = do_sync(f"{test_path}/tap-sample-incremental-rows.json", config_filename)

    assert 'version": 1574426993906' in stdout[0]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 12)

    # New schema, needs to update table schema
    stdout, _ = do_sync(f"{test_path}/tap-sample-new-schema.json", config_filename)

    assert 'version": 1583426993906' in stdout[0]
    assert check_bigquery(
        bigquery_client,
        table,
        lambda data: len(data) == 13
        and {*data[0].keys()} == {"asset", "name", "deleted", "created_at", "updated_at", "id"},
    )

    # No actual rows in this, but the target still needs to correctly run and return the state
    stdout, _ = do_sync(f"{test_path}/tap-sample-nothing-new.json", config_filename)

    assert 'version": 1593427048885' in stdout[0]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 13)

    # Some more rows with the same schema but also a state line midstream
    stdout, _ = do_sync(
        f"{test_path}/tap-sample-incremental-rows-with-state-midstream.json", config_filename
    )

    assert 'version": 1693427999999' in stdout[0]
    assert 'version": 1693429999888' in stdout[1]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 15)

    # A new, incompatible schema which can still work (ie deleted column)
    stdout, stderr = do_sync(f"{test_path}/tap-sample-column-delete.json", config_filename)

    assert [log for log in stderr if "Gave up on updating table schema" in log]
    assert 'version": 1793427040000' in stdout[0]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 17)

    # A new, incompatible schema which will not work (ie different column type)
    stdout, stderr = do_sync(f"{test_path}/tap-sample-column-type-change.json", config_filename)

    assert not stdout
    assert [log for log in stderr if "Cannot convert value to integer" in log]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 17)

    # This case currently fails until issue fixed: https://issuetracker.google.com/issues/152476581
    # A new, incompatible schema which needs to delete and recreate the table
    # target_config_delete_table = {
    #     "project_id": project_id,
    #     "dataset_id": dataset_id,
    #     "validate_records": False,
    #     "stream_data": False,
    #     "replication_method": "HYBRID",
    #     "disable_collection": True,
    #     "delete_table_on_incompatible_schema": True,
    # }
    # config_filename_delete_table = f"target-config-{dataset_id}_delete_table.json"
    # with open(config_filename_delete_table, "w") as f:
    #     f.write(json.dumps(target_config_delete_table))

    # stdout = do_sync(
    #     f"{test_path}/tap-sample-incompatible-schema.json", config_filename_delete_table
    # )

    # os.remove(config_filename_delete_table)

    # assert 'version": 1793427040000' in stdout[0]
    # assert check_bigquery(
    #     bigquery_client,
    #     table,
    #     lambda data: len(data) == 13
    #     and {*data[0].keys()} == {"name", "deleted", "created_at", "updated_at", "id"},
    # )


def test_oversize_request_slicing(setup_bigquery_and_config, check_bigquery, do_sync):
    project_id, bigquery_client, _, dataset_id = setup_bigquery_and_config()
    table = f"{project_id}.{dataset_id}.fruitimals"

    lines = (
        [
            json.dumps(
                {
                    "type": "SCHEMA",
                    "stream": "fruitimals",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "id": {
                                "type": ["integer"],
                                "minimum": -2147483648,
                                "maximum": 2147483647,
                            },
                            "name": {"type": ["null", "string"]},
                            "decimal": {"type": ["null", "number"]},
                        },
                        "definitions": {
                            "sdc_recursive_integer_array": {
                                "type": ["null", "integer", "array"],
                                "items": {"$ref": "#/definitions/sdc_recursive_integer_array"},
                            },
                            "sdc_recursive_number_array": {
                                "type": ["null", "number", "array"],
                                "items": {"$ref": "#/definitions/sdc_recursive_number_array"},
                            },
                            "sdc_recursive_string_array": {
                                "type": ["null", "string", "array"],
                                "items": {"$ref": "#/definitions/sdc_recursive_string_array"},
                            },
                            "sdc_recursive_boolean_array": {
                                "type": ["null", "boolean", "array"],
                                "items": {"$ref": "#/definitions/sdc_recursive_boolean_array"},
                            },
                            "sdc_recursive_timestamp_array": {
                                "type": ["null", "string", "array"],
                                "format": "date-time",
                                "items": {"$ref": "#/definitions/sdc_recursive_timestamp_array"},
                            },
                            "sdc_recursive_object_array": {
                                "type": ["null", "object", "array"],
                                "items": {"$ref": "#/definitions/sdc_recursive_object_array"},
                            },
                        },
                    },
                    "key_properties": ["id"],
                    "bookmark_properties": ["id"],
                }
            ),
            json.dumps(
                {
                    "type": "STATE",
                    "value": {
                        "bookmarks": {
                            "database-public-fruitimals": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1573504566181,
                            }
                        },
                        "currently_syncing": "database-public-fruitimals",
                    },
                }
            ),
        ]
        + [
            json.dumps(
                {
                    "type": "RECORD",
                    "stream": "fruitimals",
                    "record": {"decimal": Decimal(f"{x}.1"), "id": x, "name": "Pear"},
                    "version": 1573504566181,
                    "time_extracted": "2020-03-06T14:22:46.181933Z",
                }
            )
            for x in range(11000)
        ]
        + [
            json.dumps(
                {
                    "type": "STATE",
                    "value": {
                        "bookmarks": {
                            "database-public-fruitimals": {
                                "last_replication_method": "INCREMENTAL",
                                "replication_key": "id",
                                "version": 1573504566181,
                                "replication_key_value": 12,
                            }
                        },
                        "currently_syncing": None,
                    },
                }
            )
        ]
    )

    persist_lines_hybrid(project_id, dataset_id, lines=lines, validate_records=False)

    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 11000)


def test_full_table(setup_bigquery_and_config, check_bigquery, do_sync):
    project_id, bigquery_client, config_filename, dataset_id = setup_bigquery_and_config(
        replication_method="FULL_TABLE"
    )
    table = f"{project_id}.{dataset_id}.fruitimals"

    stdout, _ = do_sync(f"{test_path}/tap-sample-full-table.json", config_filename)

    assert 'version": 1573504566181' in stdout[0]
    assert check_bigquery(bigquery_client, table, lambda data: len(data) == 6)


# This case currently fails until this is fixed: https://issuetracker.google.com/issues/152476581
# def test_hybrid_with_full_table_reset(setup_bigquery_and_config, check_bigquery, do_sync):
#     project_id, bigquery_client, config_filename, dataset_id = setup_bigquery_and_config()
#     table = f"{project_id}.{dataset_id}.fruitimals"

#     # This is the beginning of a stream, setting up a new table and populating with several rows
#     stdout = do_sync(f"{test_path}/tap-sample-first-run.json", config_filename)

#     assert 'version": 1573504566181' in stdout[0]
#     assert check_bigquery(bigquery_client, table, lambda data: len(data) == 7)

#     target_config_full_table = {
#         "project_id": project_id,
#         "dataset_id": dataset_id,
#         "validate_records": False,
#         "stream_data": False,
#         "replication_method": "FULL_TABLE",
#         "disable_collection": True,
#     }
#     config_filename_full_table = f"target-config-{dataset_id}_full_table.json"
#     with open(config_filename_full_table, "w") as f:
#         f.write(json.dumps(target_config_full_table))

#     # New schema, use full table sync this time
#     stdout = do_sync(f"{test_path}/tap-sample-full-table.json", config_filename_full_table)

#     assert 'version": 1573504566181' in stdout[0]
#     assert check_bigquery(bigquery_client, table, lambda data: len(data) == 6)

#     os.remove(config_filename_full_table)

#     # Some more rows with the same schema
#     stdout = do_sync(f"{test_path}/tap-sample-incremental-rows.json", config_filename)

#     assert 'version": 1574426993906' in stdout[0]
#     assert check_bigquery(bigquery_client, table, lambda data: len(data) == 11)
