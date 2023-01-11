#!/usr/bin/env python3

import argparse
import io
import json
import sys
import traceback

import singer

from target_bigquery.encoders import DecimalEncoder
from target_bigquery.process import process
from target_bigquery.utils import emit_state, ensure_dataset
from target_bigquery.state import State, LiteralState

logger = singer.get_logger()


def main():
    # parse command line arguments (e.g., target config file path, state, table config file path, process handler type)
    parser = argparse.ArgumentParser()  # argparse.ArgumentParser(parents=[tools.argparser])
    parser.add_argument("-c", "--config", help="Config file", required=True)
    parser.add_argument("-t", "--tables", help="Table configs file", required=False)
    parser.add_argument("-s", "--state", help="Initial state file", required=False)

    # https://stackoverflow.com/questions/15008758/parsing-boolean-values-with-argparse
    parser.add_argument('--merge_state_messages', help="Merge many state messages to construct a state file",
                        dest='merge_state_messages', action='store_true')
    parser.add_argument('--no-merge_state_messages',
                        help="Don't merge many state messages into one message. The latest state message becomes the state file.",
                        dest='merge_state_messages', action='store_false')
    parser.set_defaults(merge_state_messages=None)
    # default needs to be None. If it's None, it means it's not supplied and we need to check the config file
    # if default is True here, then setting it in config file will not work
    # in the config file, default will be True

    parser.add_argument("-ph", "--processhandler",
                        help="Defines the loading process. Partial loads by default.",
                        required=False,
                        choices=["load-job", "partial-load-job", "bookmarks-partial-load-job"],
                        default="partial-load-job"
                        )

    flags = parser.parse_args()

    # read target-config file into a dict
    with open(flags.config) as f:
        config = json.load(f)

    # target tables config (e.g, partitioning and clustering)
    table_config = flags.tables or config.get("table_config")
    tables = {}
    if table_config:
        with open(table_config) as f:
            tables = json.load(f)

    # state
    state = None
    if flags.state is not None:
        with open(flags.state) as f:
            state = json.load(f)

    # determine replication method: append, truncate or incremental
    truncate = False
    incremental = False
    if config.get("replication_method", "append").lower() == "truncate":
        truncate = True
    elif config.get("replication_method", "append").lower() == "incremental":
        incremental = True

    # arguments supplied in target config
    table_prefix = config.get("table_prefix", "")
    table_suffix = config.get("table_suffix", "")
    location = config.get("location", "US")
    validate_records = config.get("validate_records", True)
    add_metadata_columns = config.get("add_metadata_columns", True)

    # deduplication arguments
    deduplication_property = config.get("deduplication_property", "")
    deduplication_order = config.get("deduplication_order", "DESC")

    # we can pass merge state option via CLI param
    merge_state_messages_cli = flags.merge_state_messages

    # we can pass merge state option via config file per Meltano request
    merge_state_messages_config = config.get("merge_state_messages", True)

    # merge state option via CLI trumps one passed via config file
    # we need to check if CLI option was passed at all. if not, we check the config file
    merge_state_messages = merge_state_messages_cli if type(
        merge_state_messages_cli) == bool else merge_state_messages_config

    project_id, dataset_id = config["project_id"], config["dataset_id"]

    table_configs = tables.get("streams", {})
    max_cache = 1024 * 1024 * config.get("max_cache", 50)  # this is needed for partial loads

    tap_stream = io.TextIOWrapper(sys.stdin.buffer, encoding="utf-8")

    client, dataset = ensure_dataset(project_id, dataset_id, location)

    try:
        from target_bigquery.processhandler import LoadJobProcessHandler, PartialLoadJobProcessHandler, \
            BookmarksStatePartialLoadJobProcessHandler

        # determine type of process handler
        ph = None

        if flags.processhandler == "load-job":
            ph = LoadJobProcessHandler
        elif flags.processhandler == "partial-load-job":
            ph = PartialLoadJobProcessHandler
        elif flags.processhandler == "bookmarks-partial-load-job":
            ph = BookmarksStatePartialLoadJobProcessHandler
        else:
            raise Exception("Unknown process handler.")

        state_iterator = process(
            ph,
            tap_stream,
            initial_state=state,
            state_handler=State if merge_state_messages else LiteralState,
            project_id=project_id,
            dataset=dataset,
            location=location,
            truncate=truncate,
            incremental=incremental,
            validate_records=validate_records,
            table_prefix=table_prefix,
            table_suffix=table_suffix,
            add_metadata_columns=add_metadata_columns,
            table_configs=table_configs,
            max_cache=max_cache,
            deduplication_property=deduplication_property,
            deduplication_order=deduplication_order
        )

        # write a state file
        for state in state_iterator:
            emit_state(state)

    except Exception as e:
        # load errors surface here
        logger.critical(e)
        exc_type, exc_value, exc_traceback = sys.exc_info()
        logger.critical(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))
        return 2  # sys.exit(2)

    return 0  # sys.exit(0)


if __name__ == "__main__":
    ret = main()
    sys.exit(ret)
