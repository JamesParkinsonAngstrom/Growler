from __future__ import annotations

import pyarrow as pa
import pyarrow.compute as pc


def project_nulls_downward(table: pa.Table) -> pa.Table:
    new_columns = []
    for name in table.column_names:
        new_columns.append(_project_column(table[name]))
    return pa.table(new_columns, names=table.column_names)


def _project_column(col):
    if isinstance(col, pa.ChunkedArray):
        if col.num_chunks == 0:
            return col
        chunks = [_project_array(c) for c in col.chunks]
        return pa.chunked_array(chunks, type=chunks[0].type)
    return _project_array(col)


def _project_array(arr):
    if pa.types.is_struct(arr.type):
        own_null = arr.is_null()
        new_children = [
            _mask_with(arr.field(i), own_null) for i in range(arr.type.num_fields)
        ]
        return pa.StructArray.from_arrays(
            new_children,
            fields=list(arr.type),
            mask=own_null,
        )
    return arr


def _mask_with(arr, parent_null):
    if pa.types.is_struct(arr.type):
        own_null = arr.is_null()
        combined = pc.or_kleene(parent_null, own_null)
        new_children = [
            _mask_with(arr.field(i), combined) for i in range(arr.type.num_fields)
        ]
        return pa.StructArray.from_arrays(
            new_children,
            fields=list(arr.type),
            mask=combined,
        )
    return _set_nulls_where(arr, parent_null)


def _set_nulls_where(arr, mask):
    if len(arr) == 0:
        return arr
    null_arr = pa.nulls(len(arr), type=arr.type)
    return pc.if_else(mask, null_arr, arr)
