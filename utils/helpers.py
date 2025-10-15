import logging
import json
from logging.handlers import TimedRotatingFileHandler
import clickhouse_connect
from datetime import datetime
from pytz import timezone

class ClickHouseConnector:
    """
    Cutomize logging to insert the log to clickhouse
    """
    def __init__(
        self,
        host,
        port,
        username,
        password,
        database
    ):
        super().__init__()
        self.database = database
        self.client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=username,
            password=password,
            database=database
        )

        q_array = self.client.query(f"""
            SELECT name
            FROM system.columns
            WHERE database = '{self.database}'
            AND type LIKE '%Array%'
        """)
        self.array_columns = {r[0] for r in q_array.result_rows}
        
        q_datetime = self.client.query(f"""
            SELECT name
            FROM system.columns
            WHERE database = '{self.database}'
            AND type LIKE 'Nullable(DateTime%'
        """)
        self.datetime_columns = {r[0] for r in q_datetime.result_rows}

    def sanitize_data(self, data):
        if isinstance(data, dict):
            return {k: self.sanitize_data(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.sanitize_data(v) for v in data]
        elif isinstance(data, bytes):
            return data.hex()
        else:
            return data

    def get_primary_key(self, table_name: str) -> list:
        """
        Return a list of primary key columns for the given table.
        If none found, returns ['id'] as default.
        """
        result = self.client.query(f"""
            SELECT name
            FROM system.columns
            WHERE database = '{self.database}'
              AND table = '{table_name}'
              AND is_in_primary_key = 1
            ORDER BY position
        """)
        return [r[0] for r in result.result_rows] or ["id"]

    def insert(self, table: str, data: dict):
        """
        Insert data in Clickhouse table
        """
        data = self.sanitize_data(data)
        def normalize_value(k, v):
            if k in self.array_columns and v is None:
                return []

            if isinstance(v, dict):
                return json.dumps(v)
            if isinstance(v, list):
                if k in self.array_columns:
                    return v
                return json.dumps(v)

            if k in self.datetime_columns:
                if v is None:
                    return None
                if isinstance(v, str):
                    try:
                        return datetime.fromisoformat(v.replace('Z', '+00:00'))
                    except Exception:
                        return None
                if isinstance(v, datetime):
                    return v
            return v

        cleaned_data = {k: normalize_value(k, v) for k, v in data.items()}

        self.client.insert(
            f"{self.database}.{table}",
            [list(cleaned_data.values())],
            column_names=list(cleaned_data.keys())
        )

    def update(self, table: str, data: dict, pk: list[str]):
        """
        Update data in ClickHouse
        Supports composite primary keys (list).
        """
        data = self.sanitize_data(data)
        def escape(k, v):
            if v is None:
                if k in self.array_columns:
                    return "[]"
                if k in self.datetime_columns:
                    return "NULL"
                return "''"

            if k in self.array_columns:
                if isinstance(v, str):
                    try:
                        v = json.loads(v)
                    except Exception:
                        v = [v]
                if not isinstance(v, list):
                    v = [v]
                formatted = []
                for item in v:
                    if isinstance(item, str):
                        if item.isdigit():
                            formatted.append(item)
                        else:
                            item = item.replace("\\", "\\\\").replace("'", "''")
                            formatted.append(f"'{item}'")
                    elif isinstance(item, bool):
                        formatted.append("1" if item else "0")
                    elif item is None:
                        formatted.append("NULL")
                    else:
                        formatted.append(str(item))
                return f"[{', '.join(formatted)}]"

            if k in self.datetime_columns:
                if isinstance(v, str):
                    try:
                        v = datetime.fromisoformat(v.replace('Z', '+00:00'))
                    except Exception:
                        pass
                if isinstance(v, datetime):
                    return f"toDateTime('{v.strftime('%Y-%m-%d %H:%M:%S')}')"

            if isinstance(v, bool):
                return "1" if v else "0"

            if isinstance(v, (dict, list)):
                json_str = json.dumps(v, ensure_ascii=False)
                json_str = json_str.replace("\\", "\\\\").replace("'", "''")
                return f"'{json_str}'"

            if isinstance(v, str):
                v = v.replace("\\", "\\\\").replace("'", "''")
                return f"'{v}'"

            return str(v)

        sets = ", ".join(f"{k} = {escape(k, v)}" for k, v in data.items() if k not in pk)

        where_clause = " AND ".join(f"{i} = {escape(i, data[i])}" for i in pk if i in data)

        if not where_clause:
            raise ValueError(f"Primary key values not found in data for table {table}")

        sql = f"""
            ALTER TABLE {self.database}.{table}
            UPDATE {sets}
            WHERE {where_clause}
        """
        self.client.command(sql)

    def delete(self, table: str, data: dict, pk: list[str]):
        """
        Perform DELETE using ALTER TABLE ... DELETE ...
        Supports multiple primary keys.
        """
        data = self.sanitize_data(data)
        where_clause = " AND ".join(f"{i} = {json.dumps(data[i])}" for i in pk if i in data)

        if not where_clause:
            raise ValueError(f"Primary key values not found in data for table {table}")

        sql = f"""
            ALTER TABLE {self.database}.{table}
            DELETE WHERE {where_clause}
        """
        self.client.command(sql)


class ClickHouseMessageProcessor:
    def __init__(self, connector: ClickHouseConnector):
        self.connector = connector

    def process_message(self, msg: dict):
        after = msg.get("after")
        before = msg.get("before")
        op = msg.get("op")
        source = msg.get("source", {})
        table = source.get("table")

        if not table:
            return

        pk = self.connector.get_primary_key(table)

        if op == "c":
            self.connector.insert(table, after)
        elif op == "u":
            self.connector.update(table, after, pk)
        elif op == "d":
            self.connector.delete(table, before or after, pk)


class ClickHouseLogger(logging.Handler):
    """
    Logging handler that sends logs to ClickHouse, with optional StreamHandler and file rotation.
    """
    def __init__(self, connector: ClickHouseConnector, table: str):
        super().__init__()
        self.connector = connector
        self.table = table
        self._ensure_log_table()

    def _ensure_log_table(self):
        self.connector.client.command(f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                timestamp DateTime,
                level String,
                topic String,
                partition Int32,
                offset Int64,
                message String
            )
            ENGINE = MergeTree()
            ORDER BY (timestamp, topic)
        """)

    def emit(self, record: logging.LogRecord):
        try:
            ts = datetime.now(timezone("Asia/Jakarta"))
            row = [
                ts,
                record.levelname,
                getattr(record, "topic", ""),
                getattr(record, "partition", -1),
                getattr(record, "offset", -1),
                record.getMessage(),
            ]
            self.connector.client.insert(
                f"{self.connector.database}.{self.table}",
                [row],
                column_names=["timestamp", "level", "topic", "partition", "offset", "message"],
            )
        except Exception as e:
            print(f"[ClickHouseLogger ERROR] {e}")

    @classmethod
    def init_logger(cls, connector: ClickHouseConnector, table: str, log_level=logging.INFO):
        """
        Create a ready to use logger that logs to both console and ClickHouse.

        Example:
            logger = ClickHouseLogger.init_logger(connector)
        """
        logger = logging.getLogger("KafkaConsumer")
        logger.setLevel(log_level)

        # file_handler = TimedRotatingFileHandler("consumer.log", when="midnight", interval=1, backupCount=7)
        # file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        # logger.addHandler(file_handler)

        ch_handler = cls(connector, table)
        ch_handler.setLevel(log_level)
        logger.addHandler(ch_handler)

        return logger