from pyarrow_utils import PyArrowTableBuffer
import pyarrow as pa


def test_PyArrowTableBuffer():
    col_names = ('Foo', 'Bar')
    dts = (pa.string(), pa.int32())
    ptb = PyArrowTableBuffer(col_names, dts)

    ptb.add(('baz', 5235))
    ptb.add(('buz', 7231))
    ptb.flush()

    ptb.add(('boo', 83673))
    ptb.add(('bugf', 37321))
    ptb.flush()

    exp = {'Bar': [5235, 7231, 83673, 37321], 'Foo': ['baz', 'buz', 'boo', 'bugf']}

    pd = ptb.to_arrow_table().to_pydict()
    assert pd == exp
