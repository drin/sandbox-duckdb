#!/usr/bin/env python

# ------------------------------
# License

# Copyright 2023 Aldrin Montana
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#    http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Interact with gene expression data as a duckdb database.

Currently supported:
    - Creating an in-memory duckdb instance
    - Loading data from MTX-formatted files
"""


# ------------------------------
# Dependencies

# >> Standard lib
import itertools

from collections import defaultdict
from pathlib import Path

# >> Third-party libs
import duckdb


# ------------------------------
# Functions

def ParseIDs(mtx_metafile: Path):
    """
    Parses the MTX-formatted file containing metadata of the expression matrix. The
    metadata is either for columns or rows, but in both cases we are interested in the
    first column (if there are many).
    """

    metadata = {}

    with mtx_metafile.open() as file_handle:
        for ndx, line in enumerate(file_handle, start=1):
            metadata[ndx] = line.strip().split('\t')[0]

    return metadata


def StreamMatrixData(mtx_datafile: Path, batch_size: int = 1024):
    # A convenience function to normalize each row of the matrix as we iterate
    def normalize(matrix_row):
        return int(matrix_row[0]), int(matrix_row[1]), float(matrix_row[2])

    batch_stopndx , row_batch = 0, []
    with mtx_datafile.open() as file_handle:
        for ndx, line in enumerate(file_handle):
            stripped_line = line.strip()

            if stripped_line.startswith('%'): continue

            # mark the last row of the batch
            if not batch_stopndx:
                batch_stopndx = ndx + (batch_size - 1)

            # if batch is complete, yield and reset
            if ndx >= batch_stopndx:
                yield row_batch
                batch_stopndx = 0
                row_batch.clear()

            # Accumulate row into current batch
            row_batch.append(normalize(stripped_line.split(' ')))

    # yield remaining rows
    yield row_batch


# ------------------------------
# Classes

class ExprDB:
    default_dbpath = 'resources/exprdb.duckdb'

    @classmethod
    def InMemory(cls):
        """ Initialize an ExprDB instance as a new in-memory duckdb instance. """
        return cls(duckdb.connect(database=':memory:'))

    @classmethod
    def AsFile(cls, db_filepath=default_dbpath):
        """ Initialize an ExprDB instance as a new file-backed duckdb instance. """
        return cls(duckdb.connect(database=db_filepath, read_only=False))

    @classmethod
    def Exists(cls, db_filepath=default_dbpath):
        """ Checks if the database at :db_filepath: exists. """
        return Path(db_filepath).is_file()

    def __init__(self, db_conn, **kwargs):
        """
        Initialize the ExprDB instance given :db_conn:. It is expected that ExprDB will be
        instantiated using a builder classmethod such as `InMemory`, for example:
            db = ExprDB.InMemory()
        """

        super().__init__(**kwargs)
        self.__dbconn   = db_conn
        self._sequences = {}

    def CreateSequence(self, sequence_name, seq_incr=1, seq_start=1):
        # >> Drop the sequence; ignore any exception from this
        try:
            self.__dbconn.execute(f'DROP SEQUENCE {sequence_name}')
        except:
            print(f'No sequence "{sequence_name}" to drop')

        # >> Create the sequence; assume for metacluster_id
        self.__dbconn.execute(
            f' CREATE SEQUENCE {sequence_name}'
            f'     INCREMENT BY {seq_incr}'
            f'     START WITH {seq_start}'
            f'     NO CYCLE'
        )

    def CreateExprData(self, table='expr'):
        self.__dbconn.execute(
            f' CREATE OR REPLACE TABLE {table} ('
            f'     gene_id VARCHAR'
            f'    ,cell_id VARCHAR'
            f'    ,expr    DOUBLE'
            f'    ,PRIMARY KEY(gene_id, cell_id)'
            f')'
        )

    def CreateClusterData(self, table='clusters'):
        # >> Create the table itself
        self.__dbconn.execute(
            f' CREATE OR REPLACE TABLE {table} ('
            f'     metacluster_id INT'
            f'    ,cluster_id     INT'
            f'    ,cell_id        VARCHAR'
            f'    ,dataset_name   VARCHAR'
            f'    ,PRIMARY KEY(metacluster_id, cluster_id, cell_id)'
            f'    ,UNIQUE(cell_id, dataset_name)'
            f')'
        )

    def InsertData(self, tuple_list, table_name):
        """ Convenience method for inserting many tuples into a table.  """

        self.__dbconn.execute(
            f" INSERT INTO {table_name} VALUES {','.join(tuple_list)}"
        )

    def InsertExprData(self, expr_tuples, table='expr'):
        """ Wrapper for InsertData using the 'expr' table. """

        self.InsertData(expr_tuples, table)

    def InsertClusterData(self, cluster_tuples, table='clusters'):
        """ Wrapper for InsertData using the 'clusters' table. """

        self.InsertData(cluster_tuples, table)

    def SequenceNext(self, sequence_name):
        """
        NOTE: Not yet used
        Convenience method that calls nextval on a database sequence and returns the next
        values for the sequence using a local counter. If the local counter is not yet
        set, it is initialized to the database sequence value.
        """

        # >> Call nextval on the DB sequence
        self.__dbconn.execute(f"SELECT nextval('{sequence_name}')")

        # >> Use local variables to cache the value
        if sequence_name not in self._sequences:
            seq_val = self.__dbconn.fetchall()[0]
            self._sequences[sequence_name] = itertools.count(start=seq_val)

        return next(self._sequences[sequence_name])

    def SequenceCurrent(self, sequence_name):
        """
        NOTE: Not yet used
        Convenience method that uses locally counters to view current sequence values.
        """

        if sequence_name not in self._sequences:
            return None

        # TODO: this doesn't quite return the "current value" of the sequence
        return self._sequences.get(sequence_name)

    def ScanExpr(self, table='expr', limit_size=20):
        self.__dbconn.execute(
            f' SELECT *'
            f' FROM   {table}'
            f' LIMIT  {limit_size}'
        )

        return self.__dbconn.fetchall()

    def ScanClusters(self, table='clusters', limit_size=20):
        self.__dbconn.execute(
            f' SELECT *'
            f' FROM   {table}'
            f' LIMIT  {limit_size}'
        )

        return self.__dbconn.fetchall()

    def QueryData(self, query_str):
        self.__dbconn.execute(query_str)
        return self.__dbconn.fetchall()

    def LoadMTX(self, mtx_dirpath: Path, mtx_basename: str):
        """
        Loads expression data from MTX-formatted files.
        :mtx_dirpath: specifies a root directory for the MTX files, :mtx_basename:
        specifies the name to use for the dataset as well as the prefix for the MTX files
        for the dataset.
        """

        dirpath         = Path(mtx_dirpath)
        filepath_prefix = f'{mtx_basename}.aggregated_filtered_counts'

        col_meta = ParseIDs(dirpath / f'{filepath_prefix}.mtx_cols')
        row_meta = ParseIDs(dirpath / f'{filepath_prefix}.mtx_rows')

        # for each slice of the expression matrix
        for matrix_batch in StreamMatrixData(dirpath / f'{filepath_prefix}_matrix.mtx'):
            tuple_list = []

            # map row and column indices to their actual values
            for matrix_row in matrix_batch:
                row_id = row_meta[matrix_row[0]]
                col_id = col_meta[matrix_row[1]]

                if not row_id:
                    print(f'Empty gene ID: {row_id}')

                tuple_list.append(f"('{row_id}', '{col_id}', {matrix_row[2]})")

            # load the slice into the database
            self.InsertExprData(tuple_list)

    def LoadClusters(self, dir_path: Path, file_name: str = 'clusters.tsv'):
        """
        Loads cluster associations from a TSV-formatted file. The file is expected to have
        two columns: <cluster ID> | <column name>

        The cluster ID column is expected to be an integer ID that is unique within the
        dataset. The "column name" column is expected to be a string ID that corresponds
        to the ID of a single-cell in the MTX-formatted data files.
        """

        cluster_filepath = dir_path / file_name

        # >> Create a dictionary mapping a cluster ID to a list of single-cell IDs
        cluster_tuples = []
        with cluster_filepath.open() as file_handle:
            for line in file_handle:
                # "mcluster" is meta cluster; "dcluster" is dataset cluster
                mcluster_id, dcluster_id, cell_id = line.strip().split('\t')
                cluster_tuples.append(
                    '('
                    f'  {int(mcluster_id.strip())}'
                    f' ,{int(dcluster_id.strip())}'
                    f" ,'{cell_id.strip()}'"
                    f" ,'{dir_path.name}'"
                    ')'
                )

        # >> Use the dictionary to populate the 'clusters' table
        self.InsertClusterData(cluster_tuples)
