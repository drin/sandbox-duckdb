#!/usr/bin/env python

# ------------------------------
# Dependencies

# >> Standard lib
from pathlib import Path
import time

# >> Third-party libs
import duckdb

# >> Internal modules
from sandbox.expression import ExprDB


# ------------------------------
# Classes

class TestLoad:
    # >> Database instance
    expr_db = None

    # >> dataset names to load
    dataset_names = [
         'E-GEOD-100618' # (metacluster 12)
        ,'E-GEOD-106540' # (metacluster 13)

        # This one is bigger and slower
        # ,'E-GEOD-76312' # (metacluster 12)
    ]

    # >> locations of each dataset above
    mtx_dirpaths  = [
         Path('resources/sample-data/ebi')
        ,Path('resources/sample-data/ebi')

        # ,Path('resources/sample-data/ebi')
    ]

    @classmethod
    def setup_class(cls):
        """ Setup suite by creating duckdb instance.  """

        # check if the database exists first
        should_createtables = not ExprDB.Exists()
        cls.expr_db = ExprDB.AsFile()

        # if the database didn't exist, create the tables
        if should_createtables:
            cls.expr_db.CreateExprData()
            cls.expr_db.CreateClusterData()

    # The docs say there's a close function but the library doesn't have it
    # @classmethod
    # def teardown_class(cls):
    #     cls.expr_db.close()

    def test_sample(self):
        table_data = self.expr_db.ScanClusters()
        assert len(table_data) == 0

        data_excerpt = '\n\t'.join(map(str, table_data))
        print(f'Excerpt: {data_excerpt}')

    def test_load(self):
        """ Load the data and specify the meta clusters. """

        for ndx, dataset_name in enumerate(self.dataset_names):
            dataset_dirpath  = self.mtx_dirpaths[ndx] / dataset_name
            print(f'[{dataset_dirpath}]:')
            print(f'\t[{dataset_name}] |>')

            # >> First, load the expression data
            print('\t\texpression...')
            self.load_expression(dataset_name, dataset_dirpath)

            # >> Then, load the cluster data
            print('\t\tclusters...')
            self.load_clusters(dataset_name, dataset_dirpath)

    def load_expression(self, name, dirpath):
        tstart = time.time()
        self.expr_db.LoadMTX(dirpath, name)
        tstop = time.time()
        print(f'\t\tElapsed time: {tstop - tstart}')

        data_excerpt = '\n\t'.join(map(str, self.expr_db.ScanExpr()))
        print(f'\t\t{data_excerpt}')

    def load_clusters(self, name, dirpath):
        tstart = time.time()
        self.expr_db.LoadClusters(dirpath)
        tstop = time.time()
        print(f'\t\tElapsed time: {tstop - tstart}')

        data_excerpt = '\n\t'.join(map(str, self.expr_db.ScanClusters()))
        print(f'\t\t{data_excerpt}')
