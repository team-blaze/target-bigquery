#!/usr/bin/env python3

import argparse
import io
import sys
import simplejson as json
import logging
import collections
import threading
import http.client
import urllib
import pkg_resources
from decimal import Decimal
from datetime import datetime, timedelta
from time import sleep

from jsonschema import validate
import singer

from oauth2client import tools
from tempfile import TemporaryFile

from google import api_core
from google.cloud import bigquery
from google.cloud.bigquery import (
    Dataset,
    WriteDisposition,
    SchemaUpdateOption,
    SchemaField,
    LoadJobConfig,
)
from google.cloud.bigquery.job import SourceFormat
from google.api_core import exceptions

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/bigquery.insertdata",
]
CLIENT_SECRET_FILE = "client_secret.json"
APPLICATION_NAME = "Singer BigQuery Target"
TABLE_CREATION_PAUSE = 30

# Error reasons below from: https://cloud.google.com/bigquery/docs/error-messages#errortable
RETRYABLE_ERROR_CODES = [
    "backendError",
    "blocked",
    "internalError",
    "quotaExceeded",
    "rateLimitExceeded",
    "stopped",
    "tableUnavailable",
]

StreamMeta = collections.namedtuple(
    "StreamMeta", ["schema", "key_properties", "bookmark_properties"]
)


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug(f"Emitting state {line}")
        sys.stdout.write(f"{line}\n")
        sys.stdout.flush()


def clear_dict_hook(items):
    return {k: v if v is not None else "" for k, v in items}


def define_schema(field, name, ignore_required=False):
    schema_name = name
    schema_type = "STRING"
    schema_mode = "NULLABLE"
    schema_description = None
    schema_fields = ()

    if "type" not in field and "anyOf" in field:
        for types in field["anyOf"]:
            if types["type"] != "null":
                field = types

    if isinstance(field["type"], list):
        if field["type"][0] != "null" and not ignore_required:
            schema_mode = "REQUIRED"
        schema_type = field["type"][-1]
    else:
        schema_type = field["type"]

    if schema_type == "object":
        schema_type = "RECORD"
        schema_fields = tuple(build_schema(field, ignore_required=ignore_required))
    if schema_type == "array":
        # TODO this is a hack instead we should use $ref
        schema_type = field.get("items").get("type", "string")
        # The 2 lines below are from https://github.com/RealSelf/target-bigquery/pull/17/files
        if isinstance(schema_type, list):
            schema_type = schema_type[-1]
        schema_mode = "REPEATED"
        if schema_type == "object":
            schema_type = "RECORD"
            schema_fields = tuple(build_schema(field.get("items"), ignore_required=ignore_required))

    if schema_type == "string":
        if "format" in field:
            if field["format"] == "date-time":
                schema_type = "timestamp"

    if schema_type == "number":
        schema_type = "FLOAT"

    return (schema_name, schema_type, schema_mode, schema_description, schema_fields)


def build_schema(schema, ignore_required=False):
    bigquery_schema = []
    for key in schema["properties"].keys():
        if not (bool(schema["properties"][key])):
            # if we endup with an empty record.
            continue

        bigquery_schema.append(
            SchemaField(
                *define_schema(schema["properties"][key], key, ignore_required=ignore_required)
            )
        )

    return bigquery_schema


def persist_lines_job(project_id, dataset_id, lines=None, truncate=False, validate_records=True):
    state = None
    schemas = {}
    rows = {}

    bigquery_client = bigquery.Client(project=project_id)

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(
                        msg.stream
                    )
                )

            schema = schemas[msg.stream]

            if validate_records:
                validate(msg.record, schema)

            # NEWLINE_DELIMITED_JSON expects JSON string data, with a newline splitting each row.
            rows[msg.stream].write(bytes(json.dumps(msg.record) + "\n", "UTF-8"))

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug("Setting state to {}".format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream
            schemas[table] = msg.schema
            rows[table] = TemporaryFile(mode="w+b")

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    for table in rows.keys():
        table_ref = bigquery_client.dataset(dataset_id).table(table)
        SCHEMA = build_schema(schemas[table])

        load_config = LoadJobConfig()
        load_config.schema = SCHEMA
        load_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON

        if truncate:
            load_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        else:
            load_config.schema_update_options = [SchemaUpdateOption.ALLOW_FIELD_ADDITION]

        load_job = bigquery_client.load_table_from_file(
            rows[table], table_ref, job_config=load_config, rewind=True
        )
        logger.info(
            f"Loading '{table}' to BigQuery as job '{load_job.job_id}'", extra={"stream": table}
        )

        try:
            load_job.result()
        except Exception as e:
            logger.error(
                f"Error on inserting to table '{table}': {str(e)}", extra={"stream": table}
            )
            return

        logger.info(f"Loaded {load_job.output_rows} row(s) to '{table}'", extra={"stream": table})

    return state


def persist_lines_stream(project_id, dataset_id, lines=None, validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}

    bigquery_client = bigquery.Client(project=project_id)

    dataset_ref = bigquery_client.dataset(dataset_id)
    dataset = Dataset(dataset_ref)
    try:
        dataset = bigquery_client.create_dataset(Dataset(dataset_ref)) or Dataset(dataset_ref)
    except exceptions.Conflict:
        pass

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(
                        msg.stream
                    )
                )

            schema = schemas[msg.stream]

            if validate_records:
                validate(msg.record, schema)

            errors[msg.stream] = bigquery_client.insert_rows_json(tables[msg.stream], [msg.record])
            rows[msg.stream] += 1

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug("Setting state to {}".format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream
            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            tables[table] = bigquery.Table(
                dataset.table(table), schema=build_schema(schemas[table])
            )
            rows[table] = 0
            errors[table] = None
            try:
                tables[table] = bigquery_client.create_table(tables[table])
                logger.info(f"Sleeping for {TABLE_CREATION_PAUSE} after creating a new table")
                sleep(TABLE_CREATION_PAUSE)
            except exceptions.Conflict:
                pass

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception("Unrecognized message {}".format(msg))

    for table in errors.keys():
        if not errors[table]:
            logger.info(
                "Loaded {} row(s) into {}:{}".format(
                    rows[table], dataset_id, table, tables[table].path
                )
            )
        else:
            logger.error("Errors:", errors[table], sep=" ")

    return state


def persist_lines_hybrid(
    project_id, dataset_id, lines=None, validate_records=True, location=None, can_delete_table=False
):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    updated_tables = {}
    rows = {}
    errors = {}
    failed_lines = []

    bigquery_client = bigquery.Client(project=project_id)
    dataset_ref = f"{project_id}.{dataset_id}"
    dataset = bigquery.Dataset(dataset_ref)
    if location:
        dataset.location = location
    bigquery_client.create_dataset(dataset, exists_ok=True)

    def write_rows_to_bigquery(streams, emit_state_after_write=False):
        nonlocal failed_lines
        for stream in streams:
            if rows[stream]:
                # By using `insert_rows_json` and passing generated `row_ids` we avoid duplication
                ids = [
                    "-".join(str(row[val]) for val in key_properties[stream])
                    for row in rows[stream]
                ]

                # Singer uses Decimal in the deserialised data which `insert_rows_json` can't
                # serialise with the built in `json` class so we need to fix it
                fixed_rows = [
                    {k: (float(v) if isinstance(v, Decimal) else v) for (k, v) in row.items()}
                    for row in rows[stream]
                ]

                # NOTE: as it turns out it takes BigQuery ~2 minutes to empty cache and acknowledge
                # a new table schema, see: https://stackoverflow.com/a/25292028/21217
                # So we allow a long retry period for recreated tables, short for incremental sync
                max_run_time = datetime.now() + timedelta(
                    seconds=300 if updated_tables.get(stream) else 30
                )
                while max_run_time > datetime.now():
                    # NOTE: This will fail if there are more than 10000 rows or the request size
                    # exceeds 10MB, see: https://cloud.google.com/bigquery/quotas#streaming_inserts
                    try:
                        errors[stream] = bigquery_client.insert_rows_json(
                            tables[stream], fixed_rows, row_ids=ids
                        )
                    except Exception as e:
                        error_string = str(e)
                        logger.warning(
                            f"Error on insert_rows_json: {error_string}", extra={"stream": stream},
                        )

                        def insert_in_halves():
                            number_of_rows = len(fixed_rows)
                            errors[stream] = bigquery_client.insert_rows_json(
                                tables[stream],
                                fixed_rows[: number_of_rows // 2],
                                row_ids=ids[: number_of_rows // 2],
                            )
                            errors[stream] + bigquery_client.insert_rows_json(
                                tables[stream],
                                fixed_rows[number_of_rows // 2 :],
                                row_ids=ids[number_of_rows // 2 :],
                            )

                        google_sdk_errors = getattr(e, "errors", [])
                        if (
                            "payload size exceeds the limit" in error_string
                            or "too many rows present" in error_string
                        ):
                            insert_in_halves()
                        # While this is similar to the above, we want to avoid doing the json.dumps
                        # if possible, so will only do it if we didn't get the expected errors
                        elif len(json.dumps(fixed_rows)) > 9000000:
                            insert_in_halves()
                        elif (
                            google_sdk_errors
                            and google_sdk_errors[0].get("reason") in RETRYABLE_ERROR_CODES
                        ):
                            pass
                        else:
                            raise e

                    if not errors[stream]:
                        break

                    sleep(5 if updated_tables.get(stream) else 1)

                if not errors[stream]:
                    logger.info(f"Loaded {len(rows[stream])} row(s) into {tables[stream].path}")
                    if emit_state_after_write:
                        emit_state(state)
                else:
                    failed_lines = failed_lines + rows[stream]
                    logger.error(
                        f"Error loading row(s) into '{tables[stream].path}': {str(errors[stream])}",
                        extra={"stream": stream},
                    )
                    del errors[stream]

                updated_tables.pop(stream, None)
                rows[stream] = []

    for line in lines:
        try:
            msg = singer.parse_message(line)
        except json.decoder.JSONDecodeError:
            logger.warning(f"Unable to parse line: {line}")
            failed_lines.append(line)
            continue

        if isinstance(msg, singer.RecordMessage):
            if msg.stream not in schemas:
                logger.warning(
                    f"Record for stream '{msg.stream}' received before its schema!",
                    extra={"stream": msg.stream},
                )
                failed_lines.append(line)
                continue

            if validate_records:
                validate(msg.record, schemas[msg.stream])

            rows[msg.stream].append(msg.record)

            state = None

        elif isinstance(msg, singer.StateMessage):
            state = msg.value
            # We'll either get a stream name here or we need to have an empty string instead of None
            full_stream = state.get("currently_syncing") or ""
            stream = full_stream.split("-")[-1]
            logger.debug(f"Setting state to: {state}", extra={"stream": stream})

            # If we already have some rows to be written and get a new state we need to write
            if rows.get(stream):
                write_rows_to_bigquery([stream], emit_state_after_write=True)

            # If stream in `bookmarks` doesn't have `replication_key_value` we assume this state is
            # a first one for a particular stream and recreate table.
            # See: https://github.com/singer-io/tap-mysql#incremental
            rep_key = state.get("bookmarks", {}).get(full_stream, {}).get("replication_key_value")
            # NOTE: this will only work if `SchemaMessage` already received before
            if (
                stream
                and not rep_key
                and not tables[stream].schema == build_schema(schemas[stream], ignore_required=True)
            ):
                table_ref = f"{dataset_ref}.{stream}"

                max_run_time = datetime.now() + timedelta(seconds=300)
                while max_run_time > datetime.now():
                    # First let's try to update the schema in the existing table
                    try:
                        logger.info(f"Updating table schema: {table_ref}", extra={"stream": stream})
                        tables[stream] = bigquery_client.update_table(
                            bigquery.Table(
                                table_ref,
                                schema=build_schema(schemas[stream], ignore_required=True),
                            ),
                            ["schema"],
                        )
                        logger.info(
                            f"Updated table '{tables[stream]}' schema: {tables[stream].schema}",
                            extra={"stream": stream},
                        )

                        # Mark the table updated so we know we need to retry inserting rows
                        updated_tables[stream] = True
                        break
                    except Exception as e:
                        error_string = str(e)
                        logger.warning(
                            f"Error on updating table schema: {error_string}",
                            extra={"stream": stream},
                        )

                        # If the update didn't work we can either retry or delete and recreate table
                        google_sdk_errors = getattr(e, "errors", [])
                        if (
                            google_sdk_errors
                            and google_sdk_errors[0].get("reason") in RETRYABLE_ERROR_CODES
                        ):
                            pass
                        elif can_delete_table and "Provided Schema does not match" in error_string:
                            bigquery_client.delete_table(table_ref)
                            logger.info(f"Deleted table: {table_ref}", extra={"stream": stream})

                            tables[stream] = bigquery_client.create_table(
                                bigquery.Table(
                                    table_ref,
                                    schema=build_schema(schemas[stream], ignore_required=True),
                                )
                            )
                            logger.info(
                                f"Created table '{tables[stream]}' schema: {tables[stream].schema}",
                                extra={"stream": stream},
                            )
                            logger.info(f"Sleeping for {TABLE_CREATION_PAUSE} after creating a new table")
                            sleep(TABLE_CREATION_PAUSE)

                            # Mark the table updated so we know we need to retry inserting rows
                            updated_tables[stream] = True
                            break
                        else:
                            logger.warning(
                                f"Gave up on updating table schema with error: {error_string}",
                                extra={"stream": stream},
                            )
                            break

        elif isinstance(msg, singer.SchemaMessage):
            stream = msg.stream
            schemas[stream] = msg.schema
            key_properties[stream] = msg.key_properties
            table_ref = f"{dataset_ref}.{stream}"
            try:
                tables[stream] = bigquery_client.get_table(table_ref)
            except api_core.exceptions.NotFound:
                # This will happen on the very first run
                tables[stream] = bigquery_client.create_table(
                    bigquery.Table(
                        table_ref, schema=build_schema(schemas[stream], ignore_required=True)
                    )
                )
                logger.info(f"Sleeping for {TABLE_CREATION_PAUSE} after creating a new table")
                sleep(TABLE_CREATION_PAUSE)

            rows[stream] = []
            errors[stream] = []

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            logger.warning(f"Unrecognized message: {msg}")
            failed_lines.append(msg)

    # We shouldn't have any rows left to write, but let's try just in case
    write_rows_to_bigquery(rows.keys())

    if failed_lines:
        logger.error(f"Number of failed lines: {len(failed_lines)}")
        # TODO: maybe also write failed_lines to special table in BigQuery
        try:
            # NOTE: if we have too many failed lines we might not be able to write the log entry
            logger.error(f"Failed lines: {str(failed_lines)}")
        except Exception:
            pass

        state = None

    bigquery_client.close()

    return state


def collect():
    try:
        version = pkg_resources.get_distribution("target-bigquery").version
        conn = http.client.HTTPConnection("collector.singer.io", timeout=10)
        conn.connect()
        params = {
            "e": "se",
            "aid": "singer",
            "se_ca": "target-bigquery",
            "se_ac": "open",
            "se_la": version,
        }
        conn.request("GET", "/i?" + urllib.parse.urlencode(params))
        conn.getresponse()
        conn.close()
    except Exception as e:
        logger.debug(f"Collection request failed with error: {e}")


def main():
    try:
        parser = argparse.ArgumentParser(parents=[tools.argparser])
        parser.add_argument("-c", "--config", help="Config file", required=True)
        flags = parser.parse_args()
    except ImportError:
        flags = None

    with open(flags.config) as input:
        config = json.load(input)

    if not config.get("disable_collection", False):
        logger.info(
            "Sending version information to stitchdata.com. "
            + "To disable sending anonymous usage data, set "
            + 'the config parameter "disable_collection" to true'
        )
        threading.Thread(target=collect).start()

    validate_records = config.get("validate_records", True)

    input = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")

    if config.get("replication_method") == "HYBRID":
        state = persist_lines_hybrid(
            config["project_id"],
            config["dataset_id"],
            input,
            validate_records=validate_records,
            location=config.get("location"),
            # NOTE: this option shouldn't be used until this BigQuery bug is fixed:
            # https://issuetracker.google.com/issues/152476581
            can_delete_table=config.get("delete_table_on_incompatible_schema", False),
        )
    elif config.get("stream_data", True):
        state = persist_lines_stream(
            config["project_id"], config["dataset_id"], input, validate_records=validate_records
        )
    else:
        state = persist_lines_job(
            config["project_id"],
            config["dataset_id"],
            input,
            truncate=config.get("replication_method") == "FULL_TABLE",
            validate_records=validate_records,
        )

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == "__main__":
    main()
