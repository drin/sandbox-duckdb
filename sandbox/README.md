# Overview

This is generally just a repo for me to play around with using duckdb to build towards
adding duckdb as a backend for [mohair][repo-mohair].

# Getting started

I use [poetry][web-poetry] to manage python dependencies in this repo. Getting started
should be as easy as:

```bash
# change to python dir
pushd sandbox

# grab sample data files
git lfs checkout

# install python dependencies and the project itself
poetry install

# run some stand-alone scripts
poetry shell

# this populates a database
toolbox/test_load

# this queries the database populated above
toolbox/test_query

# optionally, populate the database with the test instead
# poetry run pytest
```

### Potential issues

If there are issues finding the package, be sure that `poetry install` was run. Otherwise,
feel free to path the library directly (modifying `PYTHONPATH` env variable or `sys.path`
python variable; see [here][web-pypath] for some extra information).

If there are issues populating the database, be sure the sample data files have been
checked out from git LFS using `git lfs checkout`. You may need to install git-lfs on your
system or run `git lfs install` in the checked out repository first.

If there are issues querying the database, be sure the database file can be found. By
default, it is named `exprdb.duckdb` and found in the `resources` directory. More
specifically, the path defaults to [expression.ExprDB.default_dbpath][src-exprdb-dbpath],
which can be overridden when calling [expression.ExprDB.AsFile][src-exprdb-asfile] or
[expression.ExprDB.Exists][src-exprdb-exists].


<!-- Resources -->
[repo-mohair]:       https://github.com/drin/mohair
[src-exprdb-dbpath]: https://github.com/drin/sandbox-duckdb/blob/mainline/sandbox/sandbox/expression.py#L94
[src-exprdb-asfile]: https://github.com/drin/sandbox-duckdb/blob/mainline/sandbox/sandbox/expression.py#L101-L104
[src-exprdb-exists]: https://github.com/drin/sandbox-duckdb/blob/mainline/sandbox/sandbox/expression.py#L106-L109

[web-poetry]:        https://python-poetry.org/
[web-pypath]:        https://www.devdungeon.com/content/python-import-syspath-and-pythonpath-tutorial#toc-5
