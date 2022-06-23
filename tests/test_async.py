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


if __name__ == "__main__":
    unittest.main()
