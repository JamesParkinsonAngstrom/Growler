# Design Brief: A Parquet Table Format (working title)

## 1. Problem statement

We run analytical workloads against Parquet on S3 and currently use Apache Iceberg as the table format. Iceberg causes two recurring issues:

- **Schema evolution is painful.** Writers that produce a schema slightly different from the table's current schema get rejected with "schemas don't match" errors, forcing coordinated ALTER TABLE operations and blocking independent team progress.
- **Metadata bloat.** Iceberg's snapshot model produces manifest files and metadata.json versions on every commit. For our high-commit-rate tables, this requires periodic `expire_snapshots` / `rewrite_manifests` maintenance. We do not use time travel or snapshot isolation, so we pay the cost without using the feature.

Iceberg's strengths (snapshots, time travel, multi-engine interop, rich transforms) are features we do not need. We want a simpler format that keeps Iceberg's good ideas (field IDs, partition-aware pruning) and drops what we don't use.

## 2. Goals

1. Read and write Parquet files on S3 with a lightweight metadata layer.
2. Support schema evolution where writers can add columns without coordination; table schema catches up later.
3. Query via AWS Athena (via federated query Lambda connectors).
4. No snapshots, no time travel, no periodic metadata maintenance.
5. Strict write-time validation so bad writers fail fast instead of corrupting reads.
6. Eventually open-sourceable: the design should not depend on private infrastructure.

## 3. Non-goals (v1)

- Snapshot isolation or time travel.
- Multi-engine support beyond Athena (Spark, Trino, Snowflake not supported).
- Arbitrary type promotion at runtime (type changes are offline maintenance only).
- Transactional multi-table writes.
- Partition transforms (bucket, truncate, date-part). Identity partitioning only.
- List/array schema evolution beyond "add a list, remove a list." No evolution of list element schemas in v1 beyond what struct evolution allows for the element type.
- A first-party writer library. v1 accepts any Parquet files that pass validation. A writer library is a future enhancement.

## 4. Core design decisions

### 4.1 Column identity

Columns have a stable numeric **column ID** (monotonically increasing, never reused). Names are mutable labels; IDs are identity.

- Column IDs live in the pointer file's schema, keyed by dotted path (e.g. `user.address.city`).
- The pointer file carries a `next_column_id` counter; adding a column allocates and increments.
- Dropped column IDs are not reused. If a new column is added with the same path as a dropped one, it gets a fresh ID.
- Renames change the path for a given ID without changing the ID.

### 4.2 Schema evolution invariants

**The write-time invariant:** for every file committed to the table, the file's schema must be a superset of the table's current schema. Specifically:

- Every leaf path in the table schema must exist in the file schema.
- Every leaf's type in the file must match the table's declared type (no type coercion at read time).
- Extra leaves in the file (not in the table schema) are permitted and ignored at read time.

**Table schema change rules:**

- **Adding a column:** allowed. Writers must already be producing the column in their files before the table schema is updated to include it. Historical files need not contain it.
- **Removing a column:** allowed. Metadata-only operation; data in existing files is not rewritten. Writers may continue producing the column (it will be ignored on read) but should stop.
- **Renaming a column:** allowed. Metadata-only change to the path associated with a column ID. File schemas are unaffected because ID is the identity, not name.
- **Type changes:** not allowed online. Offline maintenance mode only (table must be quiesced).

**Rationale:** this is "open to extension, closed to modification." Writers can extend freely; the table schema catches up in a controlled way. Readers are always safe because every file contains everything the reader needs.

### 4.3 Null semantics for nested types

Parquet can encode nulls at any level of a nested structure. Different writers make different choices about which intermediate levels are nullable, producing cross-file inconsistency.

**Rule: project null downward through struct boundaries, preserving list structure.**

- If a struct field is null, every scalar leaf under it reads as null.
- Lists remain lists. `user.addresses = null` yields a null (or empty) list for every query on a leaf under `addresses`. `user.addresses = []` yields an empty list. `user.addresses = [{city: null}, {city: "Paris"}]` yields `[null, "Paris"]` for a query on `user.addresses.city`.
- This means the reader does not need to know which intermediate levels the writer made nullable; null-at-any-level means null-at-leaf for scalar paths.

**Implication:** queries selecting a leaf under a list return `array<T>`, not `T`. Consumers use SQL `UNNEST` when they need flat rows.

### 4.4 Canonical Parquet encoding

The format targets pyarrow-compliance. Any Parquet file that pyarrow reads correctly, and that satisfies the schema invariant above, is acceptable.

- Validation in the write Lambda uses pyarrow to open each file and inspect its schema. Files pyarrow cannot open are rejected.
- Timestamps: INT64 with logical type `timestamp` (micros or nanos), UTC. INT96 timestamps are rejected.
- Decimals: Parquet logical `decimal` type; precision/scale must match the table schema.
- Strings: Parquet logical `string` (UTF-8 `byte_array`). Plain `byte_array` without logical type is rejected unless the table schema declares the column as binary.

A more detailed encoding spec is deferred; the above covers the main ambiguities. When we build a first-party writer, it will enforce the canonical choices internally.

### 4.5 Partitioning

- Identity partitioning only. Partition columns are a subset of the table's leaf columns.
- Partition values are stored as strings in the manifest and compared against Athena predicates for pruning.
- Files are organised in the manifest tree by partition value. Files are not required to be in a particular S3 key layout, though writers may choose to mirror partition values in the path for readability.

### 4.6 Pointer file and manifest structure

- One **pointer file** per table at a known S3 location. JSON format. Contains:
  - Format version.
  - Monotonic version counter (for CAS).
  - Table schema (list of `{column_id, path, type, nullable}`).
  - `next_column_id` counter.
  - Partition spec (list of column IDs used for partitioning, in order).
  - Inline manifest tree for small tables; pointer to a separate manifest root file for large tables.
  - Per-partition watermark map (see §4.7).
- **Manifest tree** mirrors the partition hierarchy. Each node is either an internal node (keyed by partition value, pointing to children) or a leaf node (listing files).
- Leaf manifest entries contain: `s3_uri`, `partition_values`, `row_count`, `size_bytes`, per-column `{min, max, null_count}` stats, file-schema column ID mapping.
- v1: the entire tree is inline in the pointer file. A future change will externalise subtrees once size warrants it (target: keep the pointer under 1 MB). The tree structure is designed so externalisation is additive.
- CAS on the pointer file via S3 `If-Match` conditional writes using the ETag.

### 4.7 Watermarks (optional, per partition)

- Writers may report a "max offset" (or timestamp, or sequence number — opaque to the format) per partition with each `add_files` call.
- The write Lambda tracks, per table and partition, the highest observed watermark.
- Clients can query the watermark before writing to implement at-least-once / exactly-once ingestion.
- Watermarks are advisory; the format does not enforce monotonicity. If a writer reports a lower watermark than the current value, the write Lambda keeps the higher one.

### 4.8 Architecture overview

Three Lambda functions and one client library:

1. **Write Lambda** — handles `add_files` and table-schema operations. Validates files, updates the pointer via CAS.
2. **Metadata Lambda** — Athena federation metadata handler. Reads the pointer, answers schema/partition/split questions.
3. **Record Lambda** — Athena federation record handler. Reads Parquet files for assigned splits, applies null-projection, returns Arrow batches.
4. **Client library** — thin Python library wrapping HTTPS calls to the write Lambda.

All three Lambdas are Python for v1. The record Lambda is the candidate for a future Rust rewrite once semantics are stable.

---

## 5. Component specifications

### 5.1 Pointer file format

JSON. Example:

```json
{
  "format_version": 1,
  "version": 42,
  "updated_at": "2026-04-21T10:00:00Z",
  "updated_by": "write-lambda:abc123",
  "schema": {
    "next_column_id": 17,
    "columns": [
      {"id": 1, "path": "user_id", "type": "int64", "nullable": false},
      {"id": 2, "path": "user.name", "type": "string", "nullable": true},
      {"id": 3, "path": "user.addresses", "type": "list<struct>", "nullable": true},
      {"id": 4, "path": "user.addresses.city", "type": "string", "nullable": true},
      {"id": 5, "path": "event_date", "type": "date", "nullable": false}
    ]
  },
  "partition_spec": [5],
  "watermarks": {
    "event_date=2026-04-20": 1000000,
    "event_date=2026-04-21": 1500000
  },
  "manifest_tree": {
    "type": "internal",
    "partition_column_id": 5,
    "children": {
      "2026-04-20": {
        "type": "leaf",
        "files": [
          {
            "s3_uri": "s3://bucket/table/date=2026-04-20/part-0001.parquet",
            "partition_values": {"5": "2026-04-20"},
            "row_count": 500000,
            "size_bytes": 12345678,
            "column_mapping": {"1": "user_id", "2": "user.name", "3": "user.addresses", "4": "user.addresses.city", "5": "event_date"},
            "stats": {
              "1": {"min": 1, "max": 999999, "null_count": 0},
              "5": {"min": "2026-04-20", "max": "2026-04-20", "null_count": 0}
            }
          }
        ]
      }
    }
  }
}
```

Notes:

- `type` strings use a compact notation (`int64`, `string`, `list<struct>`, etc.). Full grammar to be defined; start with the subset you need.
- `column_mapping` in each file entry maps column ID to the path used *in that file*, supporting renames. If the file was written when column 2 was called `user.full_name`, the mapping records that; the reader remaps to the current path.
- Stats are per column ID and may be absent for columns without meaningful stats (strings without stats, complex types).

### 5.2 Write Lambda

#### Responsibilities

- Accept `add_files` requests; validate and commit.
- Accept schema change requests: `add_column`, `drop_column`, `rename_column`.
- Enforce the write-time invariant.
- Update the pointer file via CAS.

#### API (HTTPS, JSON)

```
POST /tables/{table}/add_files
{
  "files": [
    {
      "s3_uri": "s3://bucket/.../part-0001.parquet",
      "partition_values": {"event_date": "2026-04-21"},
      "watermark": 1500000
    }
  ]
}
→ 200 { "version": 43 }
→ 409 { "error": "schema_mismatch", "details": {...} }
→ 409 { "error": "concurrent_update", "retry": true }
```

```
POST /tables/{table}/schema/add_column
{"path": "user.email", "type": "string", "nullable": true}
→ 200 { "column_id": 17, "version": 44 }
→ 409 { "error": "column_exists" }
```

```
POST /tables/{table}/schema/drop_column
{"path": "user.deprecated_field"}
→ 200 { "version": 45 }
```

```
POST /tables/{table}/schema/rename_column
{"old_path": "user.full_name", "new_path": "user.name"}
→ 200 { "version": 46 }
```

```
GET /tables/{table}/watermark?partition=event_date%3D2026-04-21
→ 200 { "watermark": 1500000 }
```

#### add_files pseudocode

```
def add_files(table, files):
    pointer, etag = read_pointer(table)
    table_schema = pointer["schema"]
    
    validated_entries = []
    for f in files:
        # Read Parquet footer via pyarrow
        pq_schema = pyarrow.parquet.read_schema(f["s3_uri"])
        
        # Build file's leaf path set
        file_leaves = extract_leaf_paths(pq_schema)
        
        # Check: every table-schema leaf must be present in file
        for col in table_schema["columns"]:
            if col["path"] not in file_leaves:
                raise SchemaMismatch(
                    missing=col["path"],
                    column_id=col["id"],
                    message="File missing column required by table schema. Writer must be upgraded."
                )
            if not type_matches(file_leaves[col["path"]], col["type"]):
                raise SchemaMismatch(
                    column_id=col["id"],
                    expected=col["type"],
                    got=file_leaves[col["path"]],
                )
        
        # Build column_mapping: for every column_id in table schema, record the path in this file
        # (in v1 without renames this is identity; with renames it reflects the file's actual path)
        column_mapping = {col["id"]: col["path"] for col in table_schema["columns"]}
        
        # Read Parquet stats from footer
        stats = extract_stats(f["s3_uri"], column_mapping)
        
        validated_entries.append({
            "s3_uri": f["s3_uri"],
            "partition_values": f["partition_values"],
            "row_count": pq_schema.num_rows,
            "size_bytes": get_size(f["s3_uri"]),
            "column_mapping": column_mapping,
            "stats": stats,
        })
    
    # Insert into manifest tree
    new_pointer = deepcopy(pointer)
    for entry in validated_entries:
        insert_into_manifest_tree(new_pointer["manifest_tree"], entry, new_pointer["partition_spec"])
    
    # Update watermarks
    for f in files:
        if "watermark" in f:
            key = partition_key_string(f["partition_values"])
            current = new_pointer["watermarks"].get(key, 0)
            new_pointer["watermarks"][key] = max(current, f["watermark"])
    
    new_pointer["version"] += 1
    new_pointer["updated_at"] = now_iso()
    
    # CAS write
    success = write_pointer_if_match(table, new_pointer, etag)
    if not success:
        raise ConcurrentUpdate("Retry")
    
    return new_pointer["version"]
```

#### Schema operation pseudocode

```
def add_column(table, path, type_, nullable):
    pointer, etag = read_pointer(table)
    if any(c["path"] == path for c in pointer["schema"]["columns"]):
        raise ColumnExists(path)
    
    # No validation against existing files — writers are expected to already
    # be producing this column. If they aren't, the next add_files call from
    # a lagging writer will fail the invariant and the writer will know to upgrade.
    
    col_id = pointer["schema"]["next_column_id"]
    pointer["schema"]["next_column_id"] += 1
    pointer["schema"]["columns"].append({
        "id": col_id,
        "path": path,
        "type": type_,
        "nullable": nullable,
    })
    pointer["version"] += 1
    
    if not write_pointer_if_match(table, pointer, etag):
        raise ConcurrentUpdate()
    return col_id

def drop_column(table, path):
    pointer, etag = read_pointer(table)
    pointer["schema"]["columns"] = [
        c for c in pointer["schema"]["columns"] if c["path"] != path
    ]
    # Files retain the column physically; it's just no longer in the schema.
    # column_mapping entries in existing files can keep the ID → path entry;
    # readers ignore IDs not in the current schema.
    pointer["version"] += 1
    if not write_pointer_if_match(table, pointer, etag):
        raise ConcurrentUpdate()

def rename_column(table, old_path, new_path):
    pointer, etag = read_pointer(table)
    for c in pointer["schema"]["columns"]:
        if c["path"] == old_path:
            c["path"] = new_path
            break
    else:
        raise ColumnNotFound(old_path)
    pointer["version"] += 1
    if not write_pointer_if_match(table, pointer, etag):
        raise ConcurrentUpdate()
```

### 5.3 Metadata Lambda

Implements the Athena Query Federation SDK metadata handler protocol. Since the SDK is Java-first, v1 uses the documented wire protocol directly from Python (Arrow IPC over the Athena federation request/response schema).

#### Methods

- `doListSchemaNames` → hardcoded list, or derived from a config file / Glue catalog entries.
- `doListTables(schema)` → list of table names in that schema.
- `doGetTable(schema, table)` → returns Arrow schema derived from the pointer file's table schema. Partition columns flagged.
- `getPartitions(table, constraints)` → traverse the manifest tree, pruning by partition predicates. Emit one row per surviving partition.
- `doGetSplits(table, partitions)` → for each partition, emit one split per file. Split properties include `s3_uri`, `column_mapping` (from the manifest entry), and any per-file stats useful to the record handler.

#### Partition pruning pseudocode

```
def get_partitions(table, constraints):
    pointer = read_pointer(table)
    partition_column_ids = pointer["partition_spec"]
    partition_columns = [
        col for col in pointer["schema"]["columns"]
        if col["id"] in partition_column_ids
    ]
    
    surviving = []
    walk_tree(pointer["manifest_tree"], partition_columns, constraints, surviving, path=[])
    return surviving

def walk_tree(node, partition_columns, constraints, out, path):
    if node["type"] == "leaf":
        out.append({"partition_path": path, "files": node["files"]})
        return
    
    col = find_column_by_id(partition_columns, node["partition_column_id"])
    for value, child in node["children"].items():
        if not constraint_satisfiable(col, value, constraints):
            continue  # prune this subtree
        walk_tree(child, partition_columns, constraints, out, path + [(col["path"], value)])
```

#### Schema translation

Every pointer file column maps to an Arrow field. Nested paths are reconstructed into Arrow struct/list types by grouping paths with common prefixes. Example: columns at paths `user.name`, `user.addresses`, `user.addresses.city` become an Arrow struct `user` with fields `name: string` and `addresses: list<struct<city: string>>`.

Partition columns appear in the Arrow schema as top-level fields (flagged separately for Athena).

### 5.4 Record Lambda

Reads Parquet files for assigned splits and returns rows to Athena.

#### readWithConstraint pseudocode

```
def read_split(split, requested_columns, constraints):
    s3_uri = split.properties["s3_uri"]
    column_mapping = split.properties["column_mapping"]  # column_id -> path in THIS file
    
    # Translate requested columns (by current path) to file paths via column mapping.
    # requested_columns comes from Athena and uses current table paths.
    # We need the table schema to find column_ids for those paths.
    table_schema = load_table_schema(split.properties["table"])
    file_paths_to_read = []
    for requested_path in requested_columns:
        col_id = find_id_by_path(table_schema, requested_path)
        file_path = column_mapping[str(col_id)]
        file_paths_to_read.append((requested_path, file_path))
    
    # Read only needed columns from Parquet
    arrow_table = pyarrow.parquet.read_table(
        s3_uri,
        columns=[fp for _, fp in file_paths_to_read],
    )
    
    # Rename columns in the Arrow table from file paths to requested (current) paths
    arrow_table = rename_columns(arrow_table, file_paths_to_read)
    
    # Apply null projection for nested structs (not lists)
    arrow_table = project_nulls_downward(arrow_table)
    
    # Apply non-partition predicates
    arrow_table = apply_constraints(arrow_table, constraints)
    
    # Stream batches back through the BlockSpiller equivalent
    for batch in arrow_table.to_batches():
        yield batch
```

#### Null projection pseudocode

```
def project_nulls_downward(arrow_table):
    """
    For every struct column, if a struct value is null, ensure every scalar
    leaf under it reads as null. Do not descend through list types.
    """
    new_columns = []
    for name, col in zip(arrow_table.column_names, arrow_table.columns):
        new_columns.append(project_nulls_in_array(col))
    return pyarrow.table(new_columns, names=arrow_table.column_names)

def project_nulls_in_array(arr):
    if pyarrow.types.is_struct(arr.type):
        # For each child, if parent is null, child should be null.
        # pyarrow handles this automatically for well-formed arrays, but
        # writer inconsistencies may leave child values where parent is null.
        # Force the invariant:
        parent_null_mask = arr.is_null()
        new_fields = []
        for i, field in enumerate(arr.type):
            child = arr.field(i)
            child = project_nulls_in_array(child)  # recurse
            child = apply_mask_to_null(child, parent_null_mask)
            new_fields.append(child)
        return pyarrow.StructArray.from_arrays(new_fields, fields=arr.type)
    elif pyarrow.types.is_list(arr.type):
        # Do not descend through list boundary. Lists stay as lists.
        # Element nulls are the reader's/SQL's problem, not ours.
        return arr
    else:
        return arr
```

Note: the actual implementation will need to handle Arrow's offset/validity buffers carefully. The pseudocode above is semantic; a real implementation may use `pyarrow.compute` kernels or construct arrays from buffers directly for performance.

### 5.5 Client library

Thin Python wrapper. No magic.

```python
class Client:
    def __init__(self, write_lambda_url, table):
        self._url = write_lambda_url
        self._table = table
    
    def add_files(self, files: list[dict]) -> int:
        """
        files: [{"s3_uri": ..., "partition_values": {...}, "watermark": int?}]
        Returns new table version. Raises SchemaMismatch if writer needs upgrade.
        """
        ...
    
    def get_watermark(self, partition_values: dict) -> int | None:
        ...
    
    def add_column(self, path: str, type_: str, nullable: bool = True) -> int:
        ...
    
    def drop_column(self, path: str) -> None:
        ...
    
    def rename_column(self, old_path: str, new_path: str) -> None:
        ...
```

On `SchemaMismatch`, the client exposes the structured error so the writer's operators know which column is missing from their output.

---

## 6. Build order

Suggested sequence to reach a working v1:

1. **Pointer file format + write Lambda's `add_files`.** Can be tested locally against S3 with static Parquet files. Validates the invariant and pointer CAS mechanics.
2. **Schema operations (`add_column`, `drop_column`, `rename_column`).** Extends the write Lambda; no new infrastructure.
3. **Metadata Lambda.** Implement against the Athena federation wire protocol. Test with Athena against a pre-populated pointer file.
4. **Record Lambda.** Implement `readWithConstraint` with null projection. Test end-to-end: real query through Athena against real Parquet files.
5. **Client library.** Last, because the Lambdas define the contract.
6. **Manifest externalisation (deferred).** When the pointer file size grows past ~1 MB, split subtrees out.

## 7. Open questions

- **Pointer file location convention.** `s3://bucket/{table}/_metadata/pointer.json`? Needs to be discoverable by the Lambdas given just a table name.
- **Table registration.** How does a new table get its first pointer file? A `create_table` endpoint on the write Lambda, or a CLI, or a Glue integration.
- **Athena catalog integration.** Does the metadata Lambda back a custom Athena catalog, or do tables live in Glue with a pointer-file path in their properties? The Glue route is probably friendlier for Athena DDL.
- **Offline maintenance tool.** For type changes and physical column purges. Not urgent; revisit after v1.
- **Authentication between client and write Lambda.** IAM-based via function URL with AWS_IAM auth, or API Gateway with API keys, or sigv4. Probably IAM.

## 8. Out of scope for v1 (explicit)

- Rust record Lambda (after Python v1 is stable).
- First-party Parquet writer library (after v1).
- Transforms on partitions (bucket, truncate, date-part).
- Type evolution online.
- Multi-table transactions.
- Engines other than Athena.
- Metadata externalisation beyond inline (add when needed).
- List element schema evolution.
