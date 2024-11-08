from typing import Optional, cast
import helper  # noqa
import unittest
import rqdb
import logging
import io
import rqdb.logging
from rqdb.result import BulkResult

HOSTS = ["127.0.0.1:4001", "127.0.0.1:4003", "127.0.0.1:4005"]


class Test(unittest.TestCase):
    def test_single_strong(self):
        conn = rqdb.connect(HOSTS)
        cursor = conn.cursor(read_consistency="strong")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        try:
            cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
            self.assertEqual(1, cursor.rows_affected)
            self.assertEqual(1, cursor.last_insert_id)
            self.assertIsNone(cursor.fetchone())
            cursor.execute("DELETE FROM test WHERE id = ?", (1,))
            self.assertEqual(1, cursor.rows_affected)
        finally:
            cursor.execute("DROP TABLE test")

    def test_single_weak(self):
        conn = rqdb.connect(HOSTS)
        cursor = conn.cursor(read_consistency="weak")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        try:
            cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
            self.assertEqual(1, cursor.rows_affected)
            self.assertEqual(1, cursor.last_insert_id)
            self.assertIsNone(cursor.fetchone())
            cursor.execute("SELECT value FROM test")
            self.assertEqual(cursor.fetchone(), ["hello"])
            cursor.execute("DELETE FROM test WHERE id = ?", (1,))
            self.assertEqual(1, cursor.rows_affected)
        finally:
            cursor.execute("DROP TABLE test")

    def test_single_linearizable(self):
        conn = rqdb.connect(HOSTS)
        cursor = conn.cursor(read_consistency="linearizable")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        try:
            cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
            self.assertEqual(1, cursor.rows_affected)
            self.assertEqual(1, cursor.last_insert_id)
            self.assertIsNone(cursor.fetchone())
            cursor.execute("SELECT value FROM test")
            self.assertEqual(cursor.fetchone(), ["hello"])
            cursor.execute("DELETE FROM test WHERE id = ?", (1,))
            self.assertEqual(1, cursor.rows_affected)
        finally:
            cursor.execute("DROP TABLE test")

    def test_single_none_no_freshness(self):
        conn = rqdb.connect(HOSTS)
        cursor = conn.cursor(read_consistency="none", freshness="0")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        try:
            cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
            self.assertEqual(1, cursor.rows_affected)
            self.assertEqual(1, cursor.last_insert_id)
            self.assertIsNone(cursor.fetchone())
            cursor.execute("DELETE FROM test WHERE id = ?", (1,))
            self.assertEqual(1, cursor.rows_affected)
        finally:
            cursor.execute("DROP TABLE test")

    def test_single_with_freshness(self):
        conn = rqdb.connect(HOSTS)
        cursor = conn.cursor(read_consistency="none", freshness="1m")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        try:
            cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
            self.assertEqual(1, cursor.rows_affected)
            self.assertEqual(1, cursor.last_insert_id)
            self.assertIsNone(cursor.fetchone())
            cursor.execute("DELETE FROM test WHERE id = ?", (1,))
            self.assertEqual(1, cursor.rows_affected)
        finally:
            cursor.execute("DROP TABLE test")

    def test_down_node_strong(self):
        conn = rqdb.connect(
            [*HOSTS, "127.0.0.1:1234"],
            log=rqdb.LogConfig(
                connect_timeout={"enabled": True, "level": logging.DEBUG}
            ),
        )
        cursor = conn.cursor(read_consistency="strong")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        try:
            cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
            self.assertEqual(1, cursor.rows_affected)
            self.assertEqual(1, cursor.last_insert_id)
            self.assertIsNone(cursor.fetchone())
            cursor.execute("DELETE FROM test WHERE id = ?", (1,))
            self.assertEqual(1, cursor.rows_affected)
        finally:
            cursor.execute("DROP TABLE test")

    def test_discover_leader(self):
        conn = rqdb.connect(
            ["127.0.0.1:1234", *HOSTS],
            log=rqdb.LogConfig(
                connect_timeout={"enabled": True, "level": logging.DEBUG}
            ),
        )
        leader = conn.discover_leader()
        self.assertIn(f"{leader[0]}:{leader[1]}", HOSTS)

    def test_backup(self):
        conn = rqdb.connect(HOSTS)
        cursor = conn.cursor(read_consistency="strong")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        try:
            cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
            backup = io.BytesIO()
            conn.backup(backup, raw=True)
            backup.seek(0)
            self.assertEqual(
                backup.getvalue().decode("utf-8"),
                (
                    "PRAGMA foreign_keys=OFF;\n"
                    "BEGIN TRANSACTION;\n"
                    "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);\n"
                    "INSERT INTO \"test\" VALUES(1,'hello');\n"
                    "COMMIT;\n"
                ),
            )
        finally:
            cursor.execute("DROP TABLE test")

    def test_explain(self):
        conn = rqdb.connect(HOSTS)
        cursor = conn.cursor(read_consistency="strong")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        try:
            explained = cursor.explain(
                "EXPLAIN QUERY PLAN SELECT * FROM test", out="str"
            )
            self.assertEqual(explained, "--SCAN test\n")
        finally:
            cursor.execute("DROP TABLE test")

    def test_unified2(self):
        conn = rqdb.connect(HOSTS)
        cursor = conn.cursor(read_consistency="strong")
        response = cursor.executeunified2(
            (
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
                "INSERT INTO test (value) VALUES (?), (?)",
                "SELECT value FROM test ORDER BY id DESC",
                "DROP TABLE test",
            ),
            (
                [],
                ["hello", "world"],
                [],
                [],
            ),
        )
        self.assertEqual(len(response), 4)
        self.assertEqual(response[1].rows_affected, 2)
        self.assertEqual(response[2].results, [["world"], ["hello"]])

    def test_unified3(self):
        conn = rqdb.connect(HOSTS)
        cursor = conn.cursor(read_consistency="strong")
        response = cursor.executeunified3(
            (
                ("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", []),
                (
                    "INSERT INTO test (value) VALUES (?), (?)",
                    ["hello", "world"],
                ),
                (
                    "SELECT value FROM test ORDER BY id DESC",
                    [],
                ),
                ("DROP TABLE test", []),
            ),
        )
        self.assertEqual(len(response), 4)
        self.assertEqual(response[1].rows_affected, 2)
        self.assertEqual(response[2].results, [["world"], ["hello"]])

    def test_slow_query_execute(self):
        was_slow_ctr = 0

        def on_slow_query(*args, **kwargs):
            nonlocal was_slow_ctr
            was_slow_ctr += 1

        conn = rqdb.connect(
            HOSTS,
            log=rqdb.LogConfig(
                slow_query={
                    "enabled": True,
                    "threshold_seconds": 0,
                    "method": on_slow_query,
                }
            ),
        )
        cursor = conn.cursor(read_consistency="strong")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        cursor.execute("DROP TABLE test")
        self.assertEqual(was_slow_ctr, 2)

    def test_slow_query_old_style(self):
        was_slow_ctr = 0

        def on_slow_query(
            info: rqdb.logging.QueryInfo,
            /,
            *,
            duration_seconds: float,
            host: str,
            response_size_bytes: int,
            started_at: float,
            ended_at: float,
        ):
            nonlocal was_slow_ctr
            was_slow_ctr += 1

        conn = rqdb.connect(
            HOSTS,
            log=rqdb.LogConfig(
                slow_query={
                    "enabled": True,
                    "threshold_seconds": 0,
                    "method": on_slow_query,
                }
            ),
        )
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        cursor.execute("DROP TABLE test")
        self.assertEqual(was_slow_ctr, 2)

    def test_slow_query_new_style(self):
        was_slow_ctr = 0

        def on_slow_query(
            info: rqdb.logging.QueryInfo,
            /,
            *,
            duration_seconds: float,
            host: str,
            response_size_bytes: int,
            started_at: float,
            ended_at: float,
            result: Optional[BulkResult],
        ):
            nonlocal was_slow_ctr
            was_slow_ctr += 1
            self.assertIsNotNone(result)
            assert result is not None
            self.assertIsNotNone(result.time)
            for idx, item in enumerate(result.items):
                self.assertIsNotNone(item.time, f"{idx=}")

        conn = rqdb.connect(
            HOSTS,
            log=rqdb.LogConfig(
                slow_query={
                    "enabled": True,
                    "threshold_seconds": 0,
                    "method": on_slow_query,
                }
            ),
        )
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        cursor.execute("DROP TABLE test")
        self.assertEqual(was_slow_ctr, 2)

    def test_slow_query_new_style_kwvargs(self):
        was_slow_ctr = 0

        def on_slow_query(
            info: rqdb.logging.QueryInfo,
            /,
            *,
            duration_seconds: float,
            host: str,
            response_size_bytes: int,
            started_at: float,
            ended_at: float,
            **kwargs,
        ):
            nonlocal was_slow_ctr
            was_slow_ctr += 1
            self.assertIn("result", kwargs)
            self.assertIsNotNone(kwargs["result"])

        conn = rqdb.connect(
            HOSTS,
            log=rqdb.LogConfig(
                slow_query={
                    "enabled": True,
                    "threshold_seconds": 0,
                    "method": on_slow_query,
                }
            ),
        )
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        cursor.execute("SELECT * FROM test")
        cursor.execute("DROP TABLE test")
        self.assertEqual(was_slow_ctr, 3)

    def test_slow_query_executemany2(self):
        was_slow_ctr = 0

        def on_slow_query(*args, **kwargs):
            nonlocal was_slow_ctr
            was_slow_ctr += 1

            self.assertIn("result", kwargs)
            result = cast(BulkResult, kwargs["result"])
            self.assertIsNotNone(result)
            self.assertIsNotNone(result.time)
            for idx, item in enumerate(result.items):
                self.assertIsNotNone(item.time, f"{idx=}")

        conn = rqdb.connect(
            HOSTS,
            log=rqdb.LogConfig(
                slow_query={
                    "enabled": True,
                    "threshold_seconds": 0,
                    "method": on_slow_query,
                }
            ),
        )
        cursor = conn.cursor(read_consistency="strong")
        cursor.executemany2(
            (
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
                "DROP TABLE test",
            )
        )
        self.assertEqual(was_slow_ctr, 1)

    def test_slow_query_executemany3(self):
        was_slow_ctr = 0

        def on_slow_query(*args, **kwargs):
            nonlocal was_slow_ctr
            was_slow_ctr += 1

            self.assertIn("result", kwargs)
            result = cast(BulkResult, kwargs["result"])
            self.assertIsNotNone(result)
            self.assertIsNotNone(result.time)
            for idx, item in enumerate(result.items):
                self.assertIsNotNone(item.time, f"{idx=}")

        conn = rqdb.connect(
            HOSTS,
            log=rqdb.LogConfig(
                slow_query={
                    "enabled": True,
                    "threshold_seconds": 0,
                    "method": on_slow_query,
                }
            ),
        )
        cursor = conn.cursor(read_consistency="strong")
        cursor.executemany3(
            (
                ("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", []),
                ("DROP TABLE test", []),
            )
        )
        self.assertEqual(was_slow_ctr, 1)

    def test_slow_query_executeunified2(self):
        was_slow_ctr = 0

        def on_slow_query(*args, **kwargs):
            nonlocal was_slow_ctr
            was_slow_ctr += 1

            self.assertIn("result", kwargs)
            result = cast(BulkResult, kwargs["result"])
            self.assertIsNotNone(result)
            self.assertIsNotNone(result.time)
            for idx, item in enumerate(result.items):
                self.assertIsNotNone(item.time, f"{idx=}")

        conn = rqdb.connect(
            HOSTS,
            log=rqdb.LogConfig(
                slow_query={
                    "enabled": True,
                    "threshold_seconds": 0,
                    "method": on_slow_query,
                }
            ),
        )
        cursor = conn.cursor(read_consistency="strong")
        cursor.executeunified2(
            (
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)",
                "SELECT * FROM test",
                "DROP TABLE test",
            )
        )
        self.assertEqual(was_slow_ctr, 1)

    def test_slow_query_executeunified3(self):
        was_slow_ctr = 0

        def on_slow_query(*args, **kwargs):
            nonlocal was_slow_ctr
            was_slow_ctr += 1

            self.assertIn("result", kwargs)
            result = cast(BulkResult, kwargs["result"])
            self.assertIsNotNone(result)
            self.assertIsNotNone(result.time)
            for idx, item in enumerate(result.items):
                self.assertIsNotNone(item.time, f"{idx=}")

        conn = rqdb.connect(
            HOSTS,
            log=rqdb.LogConfig(
                slow_query={
                    "enabled": True,
                    "threshold_seconds": 0,
                    "method": on_slow_query,
                }
            ),
        )
        cursor = conn.cursor(read_consistency="strong")
        cursor.executeunified3(
            (
                ("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", []),
                ("SELECT * FROM test", []),
                ("DROP TABLE test", []),
            )
        )
        self.assertEqual(was_slow_ctr, 1)

    def test_slow_query_explain(self):
        was_slow_ctr = 0

        def on_slow_query(*args, **kwargs):
            nonlocal was_slow_ctr
            was_slow_ctr += 1

        conn = rqdb.connect(
            HOSTS,
            log=rqdb.LogConfig(
                slow_query={
                    "enabled": True,
                    "threshold_seconds": 0,
                    "method": on_slow_query,
                }
            ),
        )
        cursor = conn.cursor(read_consistency="strong")
        cursor.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")
        cursor.explain("SELECT * FROM test")
        cursor.execute("DROP TABLE test")
        self.assertEqual(was_slow_ctr, 3)


if __name__ == "__main__":
    unittest.main()
