from parquet_converter.backends.pyarrow_backend import PyArrowOps

# For now we ship with PyArrow as the concrete backend.
_OPS = PyArrowOps()


def get_ops():
    return _OPS
