import json
import uuid
from datetime import datetime
from tempfile import TemporaryFile

import singer
from google.api_core import exceptions as google_exceptions
from google.cloud import bigquery
from google.cloud.bigquery import LoadJobConfig, CopyJobConfig, QueryJobConfig
from google.cloud.bigquery import WriteDisposition
from google.cloud.bigquery.job import SourceFormat
from google.cloud.exceptions import NotFound
from jsonschema import validate

from target_bigquery.encoders import DecimalEncoder
from target_bigquery.schema import build_schema, cleanup_record, format_record_to_schema

from target_bigquery.simplify_json_schema import simplify
from target_bigquery.validate_json_schema import validate_json_schema_completeness, \
    check_schema_for_dupes_in_field_names


class BaseProcessHandler(object):

    def __init__(self, logger, **kwargs):
        self.logger = logger

        self.project_id = kwargs["project_id"]
        self.dataset = kwargs["dataset"]

        self.table_prefix = kwargs.get("table_prefix") or ""
        self.table_suffix = kwargs.get("table_suffix") or ""

        # LoadJobProcessHandler kwargs
        self.truncate = kwargs.get("truncate", False)
        self.incremental = kwargs.get("incremental", False)

        # deduplication kwargs
        self.deduplication_property = kwargs.get("deduplication_property", False)
        self.deduplication_order = kwargs.get("deduplication_order", "DESC")

        self.add_metadata_columns = kwargs.get("add_metadata_columns", True)
        self.validate_records = kwargs.get("validate_records", True)
        self.table_configs = kwargs.get("table_configs", {}) or {}
        self.INIT_STATE = kwargs.get("initial_state") or {}
        # PartialLoadJobProcessHandler kwargs
        self.max_cache = kwargs.get("max_cache", 1024 * 1024 * 50)

        self.tables = {}
        self.schemas = {}
        self.key_properties = {}
        self.bq_schemas = {}
        self.bq_schema_dicts = {}

    def emit_initial_state(self):
        return True

    def handle_record_message(self, msg):
        raise NotImplementedError()

    def handle_state_message(self, msg):
        raise NotImplementedError()

    def handle_schema_message(self, msg):
        """
        Handle schema message:

            1) validate json schema
                - make sure it doesn't have instances of empty objects {}
                - make sure it doesn't have duplicate field names

            2) simplify schema (we borrowed it from target-postgres).
                Make it more uniform and get rid of anyOf (for the most part)

            3) convert JSON schema to BigQuery schema

        :param msg, SchemaMessage: JSON schema
        :return: schema, list of SchemaFields: BigQuery schema
        """
        assert isinstance(msg, singer.SchemaMessage)

        # only first schema sent per stream is saved
        if msg.stream in self.tables:
            return iter([])

        self.tables[msg.stream] = "{}{}{}".format(
            self.table_prefix, msg.stream, self.table_suffix
        )
        self.schemas[msg.stream] = msg.schema
        self.key_properties[msg.stream] = msg.key_properties

        validate_json_schema_completeness(self.schemas[msg.stream])

        check_schema_for_dupes_in_field_names(stream_name=msg.stream, schema=self.schemas[msg.stream])

        schema_simplified = simplify(self.schemas[msg.stream])
        schema = build_schema(schema=schema_simplified,
                              key_properties=msg.key_properties,
                              add_metadata=self.add_metadata_columns,
                              force_fields=self.table_configs.get(msg.stream, {}).get("force_fields", {}))
        self.bq_schema_dicts[msg.stream] = self._build_bq_schema_dict(schema)
        self.bq_schemas[msg.stream] = schema

        self.logger.info(f"{msg.stream} BigQuery schema {schema}")

        yield from ()

    def on_stream_end(self):
        yield from ()

    def _build_bq_schema_dict(self, schema):  # could move this to derived class but seems right to handle in base
        """
        Convert BigQuery schema as a list to BigQuery schema as a dictionary

        :param schema, list of BigQuery SchemaFields
        :return: schema_dict, dict. Dict of BigQuery schema fields.
            Dict key is field name
            Dict value is a dict also. It has BigQuery mode and type
        """
        schema_dict = {}
        for field in schema:
            f = field if isinstance(field, dict) else field.to_api_repr()
            schema_dict[f["name"]] = f
            if f.get("fields"):
                schema_dict[f["name"]]["fields"] = self._build_bq_schema_dict(f["fields"])
            schema_dict[f["name"]].pop("description")
            schema_dict[f["name"]].pop("name")
        return schema_dict


class LoadJobProcessHandler(BaseProcessHandler):

    def __init__(self, logger, **kwargs):
        super(LoadJobProcessHandler, self).__init__(logger, **kwargs)

        # self.truncate = kwargs.get("truncate", False)
        self.partially_loaded_streams = set()
        # self.add_metadata_columns = kwargs.get("add_metadata_columns", True)
        # self.validate_records = kwargs.get("validate_records", True)
        # self.table_configs = kwargs.get("table_configs", {}) or {}
        #
        # self.INIT_STATE = kwargs.get("initial_state") or {}
        # self.STATE = State(**self.INIT_STATE)
        self.STATE_HANDLER = kwargs.get("state_handler")
        self.STATE = self.STATE_HANDLER(**self.INIT_STATE)

        self.bq_schema_dicts = {}
        self.rows = {}

        self.client = bigquery.Client(
            project=self.project_id,
            location=kwargs.get("location", "US")
        )

    def handle_schema_message(self, msg):
        for s in super(LoadJobProcessHandler, self).handle_schema_message(msg):
            yield s

        if msg.stream not in self.rows:
            self.rows[msg.stream] = TemporaryFile(mode="w+b")

        yield from ()

    def handle_record_message(self, msg):
        """
        handle record message:
            1) clean up record - prettify field names, make sure they match BigQuery naming conventions

            2) format record to schema. Make sure that data produced by the tap matches schema produced by the tap

            3) validate record using validator feature from jsonschema library

            4) add rows with data to process handler object

        :param msg, RecordMessage: message with message.type == RECORD
        :return:
        """
        assert isinstance(msg, singer.RecordMessage)

        stream = msg.stream

        if stream not in self.schemas:
            raise Exception(f"A record for stream {msg.stream} was encountered before a corresponding schema")

        schema = self.schemas[stream]
        bq_schema = self.bq_schema_dicts[stream]
        nr = cleanup_record(schema, msg.record, force_fields=self.table_configs.get(msg.stream, {}).get("force_fields", {}))

        try:
            nr = format_record_to_schema(nr, self.bq_schema_dicts[stream])
        except Exception as e:
            extra={"record" : msg.record, "schema": schema, "bq_schema": bq_schema}
            self.logger.critical(f"Cannot format a record for stream {msg.stream} to its corresponding BigQuery schema. Details: {extra}")
            raise e



        # schema validation may fail if data doesn't match schema in terms of data types
        # in this case, we validate schema again on data which has been forced to match schema
        # nr is based on msg.record, but all data from msg.record has been forced to match schema
        if self.validate_records:
            try:
                validate(msg.record, schema)
            except Exception as e:
                validate(nr, schema)

        if self.add_metadata_columns:
            nr["_time_extracted"] = msg.time_extracted.isoformat() \
                if msg.time_extracted else datetime.utcnow().isoformat()
            nr["_time_loaded"] = datetime.utcnow().isoformat()

        data = bytes(json.dumps(nr, cls=DecimalEncoder) + "\n", "UTF-8")
        self.rows[stream].write(data)

        yield from ()

    def handle_state_message(self, msg):
        assert isinstance(msg, singer.StateMessage)

        self.STATE.merge(msg.value)

        yield from ()

    def on_stream_end(self):
        self._do_temp_table_based_load(self.rows)
        yield self.STATE

    def primary_key_condition(self, stream):
        self.logger.info(f"Primary keys: {', '.join(self.key_properties[stream])}")
        keys = [f"t.{k}=s.{k}" for k in self.key_properties[stream]]
        if len(keys) < 1:
            raise Exception(f"No primary keys specified from the tap and Incremental option selected")
        return " and ".join(keys)
    #TODO: test it with multiple ids (an array of ids, if there are multiple key_properties in JSON schema)
    #TODO: test it with dupe ids in the data
    def primary_keys(self, stream):
        keys = [f"temp.{k}" for k in self.key_properties[stream]]
        if len(keys) < 1:
            raise Exception(f"No primary keys specified from the tap and Incremental option selected")
        return " , ".join(keys)

    def partition_primary_keys(self, stream):
        keys = [f"s.{k}" for k in self.key_properties[stream]]
        if len(keys) < 1:
            raise Exception(f"No primary partition keys specified from the tap and Incremental option selected")
        return " , ".join(keys)

    def duplicate_condition(self):
        keys = "and t." + self.deduplication_property + "=s." + self.deduplication_property
        return keys

    def first_primary_key(self, stream):
        keys = self.key_properties[stream][0]
        if len(keys) < 1:
            raise Exception(f"No first primary key specified from the tap and Incremental option selected")
        return "t." + keys

    def _do_temp_table_based_load(self, rows):
        assert isinstance(rows, dict)

        loaded_tmp_tables = []
        try:
            for stream in rows.keys():
                tmp_table_name = "t_{}_{}".format(self.tables[stream], str(uuid.uuid4()).replace("-", ""))

                job = self._load_to_bq(
                    client=self.client,
                    dataset=self.dataset,
                    table_name=tmp_table_name,
                    table_schema=self.bq_schemas[stream],
                    table_config=self.table_configs.get(stream, {}),
                    # key_props=self.key_properties[stream],
                    # metadata_columns=self.add_metadata_columns,
                    truncate=True,
                    rows=self.rows[stream]
                )

                loaded_tmp_tables.append((stream, tmp_table_name))

            # copy tables to production tables
            # destination table can have dupe ids used in MERGE statement
            # new data which being appended should have no dupes

            # if new data has dupes, then MERGE will fail with a similar error:
            # INFO Primary keys: id
            # CRITICAL 400 UPDATE/MERGE must match at most one source row for each target row

            # https://stackoverflow.com/questions/50504504/bigquery-error-update-merge-must-match-at-most-one-source-row-for-each-target-r
            # https://cloud.google.com/bigquery/docs/reference/standard-sql/dml-syntax

            # If a row in the table to be updated joins with more than one row from the FROM clause,
            # then the query generates the following runtime error: UPDATE/MERGE must match at most one source row for each target row.
            for stream, tmp_table_name in loaded_tmp_tables:
                incremental_success = False
                if self.incremental:
                    self.logger.info(f"Copy {tmp_table_name} to {self.tables[stream]} by INCREMENTAL")
                    self.logger.warning(f"INCREMENTAL replication method (MERGE SQL statement) is not recommended. It might result in loss of production data, because historical records get updated during the sync operation. Instead, we recommend using the APPEND replication method, which will preserve historical data.")
                    table_id = f"{self.project_id}.{self.dataset.dataset_id}.{self.tables[stream]}"
                    try:

                        self.client.get_table(table_id)
                        column_names = [f"`{x.name}`" for x in self.bq_schemas[stream]]

                        if self.deduplication_property:

                            copy_config = CopyJobConfig()
                            copy_config.write_disposition = WriteDisposition.WRITE_TRUNCATE

                            query_duplicates = """CREATE OR REPLACE TABLE `{temp_table}` AS
                                                with last_versions as (
                                                    select {primary_keys} from `{temp_table}` temp
                                                ), dedup as(
                                                select s.*,  
                                                ROW_NUMBER() OVER (PARTITION BY {partition_keys} ORDER BY {partition_deduplication_property} {deduplication_order}) AS finish_rank  
                                                from `{temp_table}` s
                                                left join last_versions t on {primary_key_condition} 
                                                where {first_primary_key} is not null),
                                                final_table as(
                                                SELECT * FROM dedup WHERE finish_rank = 1)
                                                SELECT * EXCEPT (finish_rank) FROM final_table ;
                                    """.format(
                                temp_table=f"{self.project_id}.{self.dataset.dataset_id}.{tmp_table_name}",
                                primary_keys=str(self.primary_keys(stream)),
                                partition_keys=str(self.partition_primary_keys(stream)),
                                temp_deduplication_property=str("temp." + str(self.deduplication_property)),
                                partition_deduplication_property=str("s." + str(self.deduplication_property)),
                                deduplication_property=str(self.deduplication_property),
                                primary_key_condition=self.primary_key_condition(stream),
                                duplicate_condition=self.duplicate_condition(),
                                deduplication_order=str(self.deduplication_order),
                                first_primary_key=self.first_primary_key(stream))

                            job_config = QueryJobConfig()
                            query_job = self.client.query(query_duplicates, job_config=job_config)
                            query_job.result()

                            self.logger.info(f"Removed duplicates by attribute: {str(self.deduplication_property)} and {str(self.deduplication_order)} order.")

                        query = """MERGE `{table}` t
                                USING `{temp_table}` s
                                ON {primary_key_condition}
                                WHEN MATCHED THEN
                                    UPDATE SET {set_values}
                                WHEN NOT MATCHED THEN
                                    INSERT ({new_cols}) VALUES ({cols})
                                """.format(table=table_id,
                                           temp_table=f"{self.project_id}.{self.dataset.dataset_id}.{tmp_table_name}",
                                           primary_key_condition=self.primary_key_condition(stream),
                                           set_values=', '.join(f'{c}=s.{c}' for c in column_names),
                                           new_cols=', '.join(column_names),
                                           cols=', '.join(f's.{c}' for c in column_names))
                        job_config = QueryJobConfig()
                        query_job = self.client.query(query, job_config=job_config)
                        query_job.result()
                        self.logger.info(f'LOADED {query_job.num_dml_affected_rows} rows')
                        incremental_success = True

                    except NotFound:
                        self.logger.info(f"Table {table_id} is not found, proceeding to upload with TRUNCATE")
                        self.truncate = True
                if not incremental_success:
                    truncate = self.truncate if stream not in self.partially_loaded_streams else False
                    copy_config = CopyJobConfig()
                    if truncate:
                        copy_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
                        self.logger.info(f"Copy {tmp_table_name} to {self.tables[stream]} by FULL_TABLE")
                    else:
                        copy_config.write_disposition = WriteDisposition.WRITE_APPEND
                        self.logger.info(f"Copy {tmp_table_name} to {self.tables[stream]} by APPEND")

                    self.client.copy_table(
                        sources=self.dataset.table(tmp_table_name),
                        destination=self.dataset.table(self.tables[stream]),
                        job_config=copy_config
                    ).result()

                self.partially_loaded_streams.add(stream)
                self.rows[stream].close()  # erase the file
                self.rows[stream] = TemporaryFile(mode="w+b")

        except Exception as e:
            raise e

        finally:  # delete temp tables
            for stream, tmp_table_name in loaded_tmp_tables:
                self.client.delete_table(table=self.dataset.table(tmp_table_name), not_found_ok=True)

    def _load_to_bq(self,
                    client,
                    dataset,
                    table_name,
                    table_schema,
                    table_config,
                    # key_props,
                    # metadata_columns,
                    truncate,
                    rows):
        """
        Load data to BigQuery

        :param client, BigQuery Client object
        :param dataset, Dataset: destination dataset
        :param table_name, str:
        :param table_schema, list of BigQuery SchemaFields:
        :param table_config, dict:
        :param truncate, bool: determines if we append or truncate (FULL_TABLE replication)
        :param rows, _TemporaryFileWrapper:
        :return:
        """
        logger = self.logger
        partition_field = table_config.get("partition_field", None)
        cluster_fields = table_config.get("cluster_fields", None)
        force_fields = table_config.get("force_fields", {})

        # schema_simplified = simplify(table_schema)
        # schema = build_schema(schema_simplified, key_properties=key_props, add_metadata=metadata_columns,
        #                       force_fields=force_fields)
        load_config = LoadJobConfig()
        load_config.ignore_unknown_values = True
        load_config.schema = table_schema

        # partitioning
        if partition_field:
            load_config.time_partitioning = bigquery.table.TimePartitioning(
                type_=bigquery.table.TimePartitioningType.DAY,
                field=partition_field
            )

        # clustering
        if cluster_fields:
            load_config.clustering_fields = cluster_fields

        load_config.source_format = SourceFormat.NEWLINE_DELIMITED_JSON

        # either truncate or append
        if truncate:
            logger.info(f"Load {table_name} by FULL_TABLE (truncate)")
            load_config.write_disposition = WriteDisposition.WRITE_TRUNCATE
        else:
            logger.info(f"Appending to {table_name}")
            load_config.write_disposition = WriteDisposition.WRITE_APPEND

        logger.info("loading {} to BigQuery".format(table_name))

        load_job = None
        # run load job or raise error
        try:
            load_job = client.load_table_from_file(
                rows, dataset.table(table_name), job_config=load_config, rewind=True
            )
            logger.info("loading job {}".format(load_job.job_id))
            job = load_job.result()
            logger.info(job._properties)

            return job

        except google_exceptions.BadRequest as err:
            logger.error(
                "failed to load table {} from file: {}".format(table_name, str(err))
            )
            if load_job and load_job.errors:
                reason = err.errors[0]["reason"]
                messages = [f"{err['message']}" for err in load_job.errors]
                logger.error("reason: {reason}, errors:\n{e}".format(reason=reason, e="\n".join(messages)))
                err.message = f"reason: {reason}, errors: {';'.join(messages)}"

            raise err


class PartialLoadJobProcessHandler(LoadJobProcessHandler):

    def __init__(self, logger, **kwargs):
        super(PartialLoadJobProcessHandler, self).__init__(logger, **kwargs)

        # self.max_cache = kwargs.get("max_cache", 1024 * 1024 * 50)

    def handle_state_message(self, msg):
        assert isinstance(msg, singer.StateMessage)
        for s in super(PartialLoadJobProcessHandler, self).handle_state_message(msg):
            yield s

        if sum([self.rows[s].tell() for s in self.rows.keys()]) > self.max_cache:
            rows = {s: self.rows[s] for s in self.rows.keys() if self.rows[s].tell() > 0}
            self._do_temp_table_based_load(rows)

            yield self.STATE

    def on_stream_end(self):
        rows = {s: self.rows[s] for s in self.rows.keys() if self.rows[s].tell() > 0}
        if len(rows) == 0:
            yield self.STATE
            return

        self._do_temp_table_based_load(rows)
        yield self.STATE


class BookmarksStatePartialLoadJobProcessHandler(PartialLoadJobProcessHandler):

    def __init__(self, logger, **kwargs):
        super(BookmarksStatePartialLoadJobProcessHandler, self).__init__(logger, **kwargs)

        self.STATE_HANDLER = kwargs.get("state_handler")
        self.EMITTED_STATE = self.STATE_HANDLER(**self.INIT_STATE)

    def handle_state_message(self, msg):
        assert isinstance(msg, singer.StateMessage)
        for s in super(PartialLoadJobProcessHandler, self).handle_state_message(msg):
            yield s

        if sum([self.rows[s].tell() for s in self.rows.keys()]) > self.max_cache:
            rows = {s: self.rows[s] for s in self.rows.keys() if self.rows[s].tell() > 0}
            for stream in rows.keys():
                self._do_temp_table_based_load({stream: rows[stream]})

                self.EMITTED_STATE["bookmarks"][stream] = self.STATE["bookmarks"][stream]

                yield self.EMITTED_STATE

    def on_stream_end(self):
        rows = {s: self.rows[s] for s in self.rows.keys() if self.rows[s].tell() > 0}
        if len(rows) == 0:
            yield self.STATE
            return

        for stream in rows.keys():
            self._do_temp_table_based_load({stream: rows[stream]})

            self.EMITTED_STATE["bookmarks"][stream] = self.STATE["bookmarks"][stream]

            yield self.EMITTED_STATE
