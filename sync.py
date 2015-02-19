# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, SimpleStatement
from cassandra.cqltypes import UUIDType
import elasticsearch
import json
import calendar
from elasticsearch.helpers import bulk, streaming_bulk, scan
import sys
import datetime
from uuid import UUID
import time


class Sync:
    """ sync between elasticsearch and cassandra
    """

    last_sync = {'cs': {}, 'es': {}}

    def __init__(self, config):
        self.config = config
        self.mappings = [[opt, config.get('mappings', opt)] for opt in config.options('mappings')]

        # parameters for cassandra
        self.cs_nodes = config.get('cassandra', 'nodes')
        self.cs_keyspace = config.get('cassandra', 'keyspace')
        self.cs_tables = config.options('mappings')

        # create connection for cassandra
        self.cs_client = Cluster().connect(self.cs_keyspace)

        # parameters for elasticsearch
        self.es_nodes = config.get('elasticsearch', 'nodes')
        self.es_index = config.get('elasticsearch', 'index')
        self.es_types = [config.get('mappings', opt) for opt in config.options('mappings')]

        # create connection for elasticsearch
        self.es_client = elasticsearch.Elasticsearch(self.es_nodes)

    def get_cs_by_id(self, cf, fields, id_column, ids):
        command = "SELECT %s FROM %s WHERE %s IN (%s)" % (",".join(fields), cf, id_column, ",".join(ids))
        return self.cs_client.execute(command)

    def cs_insert(self, cf, cols):
        command = "INSERT INTO %s (%s) VALUES (%s)" % (cf, ",".join(cols), ",".join(["?"] * len(cols)))
        return self.cs_client.prepare(command)

    def cs_update(self, cf, id_column, fields):
        command = "UPDATE %s SET %s WHERE %s = ?" % (cf, ",".join([field + " = ?" for field in fields]), id_column)
        return self.cs_client.prepare(command)

    def es_range_filter(self, col, start, end):
        return {
            "filter": {
                "range": {
                    col: {
                        "gte": start,
                        "lte": end
                    }
                }
            }
        }

    def es_ids_filter(self, es_type, ids):
        return {
            "filter": {
                "ids": {
                    "type": es_type,
                    "values": ids
                }
            }
        }

    def get_es_bulk_action(self, es_type, id_column, id_value, timestamp_column, timestamp_value, data):

        if isinstance(timestamp_value, datetime.datetime):
            timestamp_value = calendar.timegm(timestamp_value.utctimetuple())

        id_value = str(id_value)
        timestamp_value = str(timestamp_value)

        data[timestamp_column] = timestamp_value

        action = {
            '_index': self.es_index,
            '_type': es_type,
            id_column: id_value,
            '_source': data
        }

        return action

    def sync_databases(self):
        """ method to synchronize the databases
        """

        for mapping in self.mappings:
            self._sync_db(mapping[0], mapping[1])

    def _sync_db(self, cs_table, es_type):
        """ this method realize the real synchronization
        """

        cst_id = self.config.get(cs_table, 'id')
        cst_timestamp = self.config.get(cs_table, 'timestamp')
        cst_columns = [cst_id, cst_timestamp]
        cst_columns.extend(self.config.get(cs_table, 'columns').split())

        est_id = self.config.get(es_type, 'id')
        est_timestamp = self.config.get(es_type, 'timestamp')
        est_columns = self.config.get(es_type, 'columns').split()

        if cs_table in self.last_sync['cs']:
            cs_start, cs_end = self.last_sync['cs'][cs_table], time.time()
        else:
            cs_start, cs_end = None, None

        if es_type in self.last_sync['es']:
            es_start, es_end = self.last_sync['es'][es_type], time.time()
        else:
            es_start, es_end = None, None

        cs_get_query = 'SELECT %s, %s FROM %s' % (cst_id, cst_timestamp, cs_table)

        range_filter = {}
        if es_start and es_end:
            range_filter = self.es_range_filter(est_timestamp, es_start, es_end)

        self.cs_client.set_keyspace(self.cs_keyspace)
        cs_data = self.cs_client.execute(cs_get_query)
        self.last_sync['cs'][cs_table] = time.time()

        es_scan = scan(
            self.es_client,
            index=self.es_index,
            doc_type=es_type,
            fields=[est_id, est_timestamp], query=range_filter
        )

        self.last_sync['es'][es_type] = time.time()
        es_data = [data for data in es_scan]

        all_data = {}

        ids_insert_cs = []
        ids_update_cs = []

        ids_insert_es = []
        ids_update_es = []

        for result in cs_data:
            res_id, res_timestamp = str(result[0]), int(calendar.timegm(result[1].utctimetuple()))
            if not (cs_start and cs_end):
                all_data[res_id] = [res_timestamp, None]
            elif cs_start and cs_end and res_timestamp >= cs_start and res_timestamp <= cs_end:
                all_data[res_id] = [res_timestamp, None]

        for document in es_data:
            if "fields" in document:
                if est_id == '_id':  # special case - is not inside fields. there must be a better way to do this ;(
                    doc_id, doc_timestamp = document[est_id], int(
                        document['fields'][est_timestamp][0])
                else:
                    doc_id, doc_timestamp = document['fields'][est_id], int(
                        document['fields'][est_timestamp][0])

                if doc_id in all_data:
                    all_data[doc_id][1] = doc_timestamp
                else:
                    all_data[doc_id] = [None, doc_timestamp]

        for uid in all_data:
            cassandra_ts, es_ts = all_data[uid]
            if cassandra_ts and es_ts:
                if cassandra_ts > es_ts:  # same id, cassandra is the most recent. update that data on es.
                    ids_update_es.append(uid)
                elif es_ts > cassandra_ts:  # same id, es is the most recent. update that data on cassandra.
                    ids_update_cs.append(uid)
            elif cassandra_ts:  # present only on cassandra. add to es.
                ids_insert_es.append(uid)
            elif es_ts:  # present only on es. add to cassandra.

                ids_insert_cs.append(uid)

        if ids_insert_es or ids_update_es:
            actions = []
            from_cassandra_to_es = self.get_cassandra_documents_by_id(
                cs_table,
                cst_columns,
                cst_id,
                ids_insert_es + ids_update_es
            )

            for document in from_cassandra_to_es:
                data = {}
                for i in range(len(cst_columns)):
                    data[est_columns[i]] = getattr(document, cst_columns[i])

                actions.append(
                    self.es_bulk_action(
                        es_type,
                        est_id,
                        getattr(document, cst_id),
                        est_timestamp,
                        getattr(document, cst_timestamp),
                        data
                    )
                )

            bulk(self.es_client, actions)  # send all inserts/updates to es at once

        if ids_insert_cs or ids_update_cs:
            batch = BatchStatement()

            ids_filter = self.es_ids_filter(es_type, ids_insert_cs + ids_update_cs)
            from_es_to_cassandra = self.es_client.search(
                index=self.es_index,
                doc_type=es_type,
                fields=est_columns + [cst_timestamp],
                body=ids_filter
            )

            for document in from_es_to_cassandra['hits']['hits']:
                if document[est_id] == '_id':
                    id_value = document[est_id]
                else:
                    id_value = document["fields"][est_id]

                es_data = [UUID(id_value), datetime.datetime.utcfromtimestamp(int(document['fields'][est_timestamp][0]))]

                for field in est_columns:
                    es_data.append(document['fields'][field][0])

                cs_insert = self.cs_insert(cs_table, cst_columns)
                cs_update = self.cs_update(cs_table, cst_id, cst_columns[1:])

                if id_value in ids_insert_cs:
                    batch.add(cs_insert, tuple(es_data))
                else:
                    batch.add(cs_update, tuple(es_data[1:] + [UUID(id_value)]))

            self.cs_client.execute(batch)
