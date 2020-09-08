from typing import Iterable
import pyarrow as pa
from ._version import __version__


"""
PyArrowTableBuffer

Write tabular data row by row into a Parquet table (PyArrow Table)
"""


class PyArrowTableBuffer:
    """
    Class to incrementally build up a PyArrow table object
    for export to Parquet
    """
    def __init__(self, col_names: Iterable[str], datatypes: Iterable[pa.DataType]):
        """
        :param Iterable col_names: names of columns
        :param Iterable datatypes: PyArrow data type of each column
        """
        assert len(col_names) == len(datatypes), "each column must have a datatype"
        self._col_names = col_names
        self._datatypes = datatypes
        self._batches = []
        self._reset_cols()

    def _reset_cols(self):
        self._cols = tuple([[] for _ in self._datatypes])

    def add(self, data: Iterable):
        """
        Add a single row of data
        Must match the length and datatypes as passed in the constructor

        :param data:
        :return:
        """
        for i, d in enumerate(data):
            self._cols[i].append(d)

    def to_arrow_record_batch(self) -> pa.RecordBatch:
        """
        Returns data currently in buffers as a PyArrow.RecordBatch
        You should call `flush()` instead of this

        :return:
        """
        return pa.RecordBatch.from_arrays(
            [pa.array(self._cols[i], type=self._datatypes[i]) for i in range(len(self._cols))],  # noqa
            self._col_names
        )

    def to_arrow_table(self) -> pa.Table:
        """
        Flushes the current set of data in RecordBatches and returns the entire
        dataset as a `PyArrow.Table`.

        The table can then be saved using ``pyarrow.parquet.write_table(table, 'foo.parquet')``
        :return:
        """
        self.flush()
        return pa.Table.from_batches(
            self._batches,  # noqa
            pa.schema(zip(self._col_names, self._datatypes))
        )

    def flush(self):
        """
        Flush the current data buffers to a record batch
        :return:
        """
        self._batches.append(self.to_arrow_record_batch())
        self._reset_cols()

