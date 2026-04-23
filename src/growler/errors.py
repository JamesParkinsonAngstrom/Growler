class GrowlerError(Exception):
    pass


class SchemaMismatch(GrowlerError):
    def __init__(self, message: str, **details):
        super().__init__(message)
        self.message = message
        self.details = details

    def to_dict(self):
        return {"error": "schema_mismatch", "message": self.message, "details": self.details}


class ConcurrentUpdate(GrowlerError):
    def to_dict(self):
        return {"error": "concurrent_update", "retry": True}


class ColumnExists(GrowlerError):
    def __init__(self, path: str):
        super().__init__(f"Column already exists: {path}")
        self.path = path

    def to_dict(self):
        return {"error": "column_exists", "path": self.path}


class ColumnNotFound(GrowlerError):
    def __init__(self, path_or_id):
        super().__init__(f"Column not found: {path_or_id}")
        self.key = path_or_id

    def to_dict(self):
        return {"error": "column_not_found", "key": str(self.key)}


class TableNotFound(GrowlerError):
    def __init__(self, table: str):
        super().__init__(f"Table not found: {table}")
        self.table = table

    def to_dict(self):
        return {"error": "table_not_found", "table": self.table}


class InvalidType(GrowlerError):
    pass
