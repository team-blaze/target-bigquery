#!/usr/bin/env python3

import argparse
import io
import sys
import json
import logging
import collections
import threading
import http.client
import urllib
import pkg_resources

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
from google.cloud.bigquery.table import TableReference
from google.api_core import exceptions

try:
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument("-c", "--config", help="Config file", required=True)
    flags = parser.parse_args()
except ImportError:
    flags = None

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)
logger = singer.get_logger()

SCOPES = [
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/bigquery.insertdata",
]
CLIENT_SECRET_FILE = "client_secret.json"
APPLICATION_NAME = "Singer BigQuery Target"

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


def define_schema(field, name):
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
        if field["type"][0] != "null":
            schema_mode = "REQUIRED"
        schema_type = field["type"][-1]
    else:
        schema_type = field["type"]
    if schema_type == "object":
        schema_type = "RECORD"
        schema_fields = tuple(build_schema(field))
    if schema_type == "array":
        # TODO this is a hack instead we should use $ref
        schema_type = field.get("items").get("type", "string")
        # The 2 lines below are from https://github.com/RealSelf/target-bigquery/pull/17/files
        if isinstance(schema_type, list):
            schema_type = schema_type[-1]
        schema_mode = "REPEATED"
        if schema_type == "object":
            schema_type = "RECORD"
            schema_fields = tuple(build_schema(field.get("items")))

    if schema_type == "string":
        if "format" in field:
            if field["format"] == "date-time":
                schema_type = "timestamp"

    if schema_type == "number":
        schema_type = "FLOAT"

    return (schema_name, schema_type, schema_mode, schema_description, schema_fields)


def build_schema(schema):
    SCHEMA = []
    for key in schema["properties"].keys():
        if not (bool(schema["properties"][key])):
            # if we endup with an empty record.
            continue

        SCHEMA.append(SchemaField(*define_schema(schema["properties"][key], key)))

    return SCHEMA


def persist_lines_job(project_id, dataset_id, lines=None, truncate=False, validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}

    bigquery_client = bigquery.Client(project=project_id)

    # try:
    #     dataset = bigquery_client.create_dataset(Dataset(dataset_ref)) or Dataset(dataset_ref)
    # except exceptions.Conflict:
    #     pass

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

            # NEWLINE_DELIMITED_JSON expects literal JSON formatted data, with a newline character splitting each row.
            dat = bytes(json.dumps(msg.record) + "\n", "UTF-8")

            rows[msg.stream].write(dat)
            # rows[msg.stream].write(bytes(str(msg.record) + '\n', 'UTF-8'))

            state = None

        elif isinstance(msg, singer.StateMessage):
            logger.debug("Setting state to {}".format(msg.value))
            state = msg.value

        elif isinstance(msg, singer.SchemaMessage):
            table = msg.stream
            schemas[table] = msg.schema
            key_properties[table] = msg.key_properties
            # tables[table] = bigquery.Table(dataset.table(table), schema=build_schema(schemas[table]))
            rows[table] = TemporaryFile(mode="w+b")
            errors[table] = None
            # try:
            #     tables[table] = bigquery_client.create_table(tables[table])
            # except exceptions.Conflict:
            #     pass

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

        if not truncate:
            load_config.schema_update_options = [SchemaUpdateOption.ALLOW_FIELD_ADDITION]

        if truncate:
            load_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

        rows[table].seek(0)
        logger.info("loading {} to Bigquery.\n".format(table))
        load_job = bigquery_client.load_table_from_file(
            rows[table], table_ref, job_config=load_config
        )
        logger.info("loading job {}".format(load_job.job_id))
        logger.info(load_job.result())

    # for table in errors.keys():
    #     if not errors[table]:
    #         print('Loaded {} row(s) into {}:{}'.format(rows[table], dataset_id, table), tables[table].path)
    #     else:
    #         print('Errors:', errors[table], sep=" ")

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


def handle_bigquery_error(err, data={}):
    # retryable = True
    # for msg in err["errors"]:
    #     if msg["reason"] not in RETRYABLE_ERROR_CODES:
    #         retryable = False

    # if retryable:
    #     logger.warning(f"Recoverable BigQuery insert error: {err}", extra={"error": err})
    #     raise RecoverableException(str(err))

    # logger.warning(
    #     f"Unrecoverable BigQuery insert error: {err}", extra={"error": err, "data": data}
    # )
    # raise UnrecoverableException(str(err))
    pass


def persist_lines_hybrid(project_id, dataset_id, lines=None, validate_records=True):
    state = None
    schemas = {}
    key_properties = {}
    tables = {}
    rows = {}
    errors = {}
    failed_lines = []

    bigquery_client = bigquery.Client(project=project_id)
    dataset_ref = bigquery_client.dataset(dataset_id)
    bigquery_client.create_dataset(Dataset(dataset_ref), exists_ok=True)

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
            # TODO: verify whether this stream name extraction method would always work
            full_stream = state.get("currently_syncing") or ""
            stream = full_stream.split("-")[-1]
            logger.debug(f"Setting state to: {state}", extra={"stream": stream})

            # If stream in `bookmarks` doesn't have `replication_key_value` we assume this state is
            # a first one for a particular stream and recreate table.
            # See: https://github.com/singer-io/tap-mysql#incremental
            rep_key = state.get("bookmarks", {}).get(full_stream, {}).get("replication_key_value")
            # NOTE: this will only work if `SchemaMessage` already received before
            if (
                stream
                and not rep_key
                and not tables[stream].schema == build_schema(schemas[stream])
            ):
                # Delete table
                table_ref = TableReference(dataset_ref, stream)
                logger.info(f"Deleting table: {table_ref}")
                bigquery_client.delete_table(table_ref)

                # Recreate table
                tables[stream] = bigquery_client.create_table(
                    bigquery.Table(table_ref, schema=build_schema(schemas[stream]))
                )
                logger.info(
                    f"Created table '{tables[stream]}' with schema: {tables[stream].schema}"
                )

        elif isinstance(msg, singer.SchemaMessage):
            stream = msg.stream
            schemas[stream] = msg.schema
            key_properties[stream] = msg.key_properties
            table_ref = TableReference(dataset_ref, stream)
            try:
                tables[stream] = bigquery_client.get_table(table_ref)
            except api_core.exceptions.NotFound:
                # This will happen on the very first run
                tables[stream] = bigquery_client.create_table(
                    bigquery.Table(table_ref, schema=build_schema(schemas[stream]))
                )
            rows[stream] = []
            errors[stream] = []

        elif isinstance(msg, singer.ActivateVersionMessage):
            # This is experimental and won't be used yet
            pass

        else:
            raise Exception(f"Unrecognized message: {msg}")
            failed_lines.append(msg)

    # TODO: This might fail if there are more than 10000 rows or the request size exceeds 10MB, see:
    # https://cloud.google.com/bigquery/quotas#streaming_inserts
    for stream in rows.keys():
        if rows[stream]:
            # By using `insert_rows_json` and passing `row_ids` we can avoid duplication
            ids = [
                "-".join(str(row[val]) for val in key_properties[stream]) for row in rows[stream]
            ]

            errors[stream] = bigquery_client.insert_rows_json(
                tables[stream], rows[stream], row_ids=ids
            )

    # NOTE: as it turns out it takes BigQuery ~2 minutes to empty its caches and acknowledge a new
    # table schema, see: https://stackoverflow.com/a/25292028/21217
    # So it means that we'll see errors for a few tries of inserts afterwards, but by adding the
    # rows to `failed_lines` we won't return state, see below
    for stream in errors.keys():
        if not errors[stream]:
            logger.info(f"Loaded {len(rows[stream])} row(s) into {tables[stream].path}")
        else:
            failed_lines = failed_lines + rows[stream]
            logger.warning(
                f"Error while loading row(s) into '{tables[stream].path}': {str(errors[stream])}",
            )

    # TODO: also try writing failed_lines to special table in BigQuery
    if failed_lines:
        logger.warning(f"Failed lines: {str(failed_lines)}", extra={"failed_lines": failed_lines})
        # NOTE: on error prevent state from being returned which means the tap will retry
        return

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
            config["project_id"], config["dataset_id"], input, validate_records=validate_records
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
