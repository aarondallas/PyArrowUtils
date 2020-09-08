#############
PyArrow Utils
#############

:Author: Aaron Dallas
:Date: September 2020

Helper code for working with Parquet files

PyArrowTableBuffer
==================

Write tabular data row by row into a Parquet table (PyArrow Table)

Synopsis
~~~~~~~~

.. code-block::python

    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyarrow_utils import PyArrowTableBuffer

    ptb = PyArrowTableBuffer(
        ('col1', 'col2'),     # column names
        (pa.int32(), pa.string) # pyarrow data types
    )

    ptb.add((1, 'foo')) # add to internal data struct
    ptb.add((2, 'bar'))
    ptb.flush()         # flushes internal data struct to PyArrow record batches

    ## ... add some more data ...

    pq.write_table(
        ptb.to_pyarrow_table(),  # calls flush()
        '/path/to/foo.parquet'
    )

