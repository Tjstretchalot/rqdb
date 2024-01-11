import helper  # noqa
import unittest
import rqdb
import logging
import asyncio
import io


HOSTS = ["127.0.0.1:4001"]


def async_test(func):
    def wrapper(*args, **kwargs):
        asyncio.run(func(*args, **kwargs))

    return wrapper


class Test(unittest.TestCase):
    @async_test
    async def test_single_strong(self):
        async with rqdb.connect_async(HOSTS) as conn:
            cursor = conn.cursor(read_consistency="strong")
            await cursor.execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"
            )
            try:
                await cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
                self.assertEqual(1, cursor.rows_affected)
                self.assertEqual(1, cursor.last_insert_id)
                self.assertIsNone(cursor.fetchone())
                await cursor.execute("DELETE FROM test WHERE id = ?", (1,))
                self.assertEqual(1, cursor.rows_affected)
            finally:
                await cursor.execute("DROP TABLE test")

    @async_test
    async def test_single_weak(self):
        async with rqdb.connect_async(HOSTS) as conn:
            cursor = conn.cursor(read_consistency="weak")
            await cursor.execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"
            )
            try:
                await cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
                self.assertEqual(1, cursor.rows_affected)
                self.assertEqual(1, cursor.last_insert_id)
                self.assertIsNone(cursor.fetchone())
                await cursor.execute("DELETE FROM test WHERE id = ?", (1,))
                self.assertEqual(1, cursor.rows_affected)
            finally:
                await cursor.execute("DROP TABLE test")

    @async_test
    async def test_single_none_no_freshness(self):
        async with rqdb.connect_async(HOSTS) as conn:
            cursor = conn.cursor(read_consistency="none", freshness="0")
            await cursor.execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"
            )
            try:
                await cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
                self.assertEqual(1, cursor.rows_affected)
                self.assertEqual(1, cursor.last_insert_id)
                self.assertIsNone(cursor.fetchone())
                await cursor.execute("DELETE FROM test WHERE id = ?", (1,))
                self.assertEqual(1, cursor.rows_affected)
            finally:
                await cursor.execute("DROP TABLE test")

    @async_test
    async def test_single_with_freshness(self):
        async with rqdb.connect_async(HOSTS) as conn:
            cursor = conn.cursor(read_consistency="none", freshness="1m")
            await cursor.execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"
            )
            try:
                await cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
                self.assertEqual(1, cursor.rows_affected)
                self.assertEqual(1, cursor.last_insert_id)
                self.assertIsNone(cursor.fetchone())
                await cursor.execute("DELETE FROM test WHERE id = ?", (1,))
                self.assertEqual(1, cursor.rows_affected)
            finally:
                await cursor.execute("DROP TABLE test")

    @async_test
    async def test_down_node_strong(self):
        async with rqdb.connect_async(
            [*HOSTS, "127.0.0.1:1234"],
            log=rqdb.LogConfig(
                connect_timeout={"enabled": True, "level": logging.DEBUG}
            ),
        ) as conn:
            cursor = conn.cursor(read_consistency="strong")
            await cursor.execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"
            )
            try:
                await cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
                self.assertEqual(1, cursor.rows_affected)
                self.assertEqual(1, cursor.last_insert_id)
                self.assertIsNone(cursor.fetchone())
                await cursor.execute("DELETE FROM test WHERE id = ?", (1,))
                self.assertEqual(1, cursor.rows_affected)
            finally:
                await cursor.execute("DROP TABLE test")

    @async_test
    async def test_discover_leader(self):
        async with rqdb.connect_async(["127.0.0.1:1234", *HOSTS]) as conn:
            leader = await conn.discover_leader()
            self.assertIn(f"{leader[0]}:{leader[1]}", HOSTS)

    @async_test
    async def test_backup(self):
        async with rqdb.connect_async(HOSTS) as conn:
            cursor = conn.cursor(read_consistency="strong")
            await cursor.execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"
            )
            try:
                await cursor.execute("INSERT INTO test (value) VALUES (?)", ("hello",))
                backup = io.BytesIO()
                await conn.backup(backup, raw=True)
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
                await cursor.execute("DROP TABLE test")

    @async_test
    async def test_explain(self):
        async with rqdb.connect_async(HOSTS) as conn:
            cursor = conn.cursor(read_consistency="strong")
            await cursor.execute(
                "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"
            )
            try:
                explained = await cursor.explain(
                    "EXPLAIN QUERY PLAN SELECT * FROM test", out="str"
                )
                self.assertEqual(explained, "--SCAN test\n")
            finally:
                await cursor.execute("DROP TABLE test")

    @async_test
    async def test_unified2(self):
        async with rqdb.connect_async(HOSTS) as conn:
            cursor = conn.cursor(read_consistency="strong")
            response = await cursor.executeunified2(
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

    @async_test
    async def test_unified3(self):
        async with rqdb.connect_async(HOSTS) as conn:
            cursor = conn.cursor(read_consistency="strong")
            response = await cursor.executeunified3(
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
