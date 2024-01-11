import helper  # noqa
import unittest
import rqdb
import logging
import io


HOSTS = ["127.0.0.1:4001"]


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


if __name__ == "__main__":
    unittest.main()
