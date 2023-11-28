import helper  # noqa
import unittest
import rqdb.preprocessing as pp


class Test(unittest.TestCase):
    def test_multiline_with(self):
        self.assertEqual(
            pp.get_sql_command(
                """
                WITH foo AS (
                    SELECT name
                    FROM bar
                    GROUP BY id
                )
                INSERT INTO melons (
                    free,
                    bar,
                    baz
                )
                SELECT
                    foo.name,
                    bar,
                    baz
                FROM foo
                JOIN baz ON baz.id = foo.id
                """
            ),
            "INSERT",
        )

    def test_named_with(self):
        self.assertEqual(
            pp.get_sql_command(
                "WITH foo(name) AS (SELECT baz FROM bar) SELECT * FROM foo"
            ),
            "SELECT",
        )

    def test_named_with_multiple_columns(self):
        self.assertEqual(
            pp.get_sql_command(
                "WITH foo(name, id) AS (SELECT baz, id FROM bar) SELECT * FROM foo"
            ),
            "SELECT",
        )

    def test_with_values(self):
        self.assertEqual(
            pp.get_sql_command("WITH foo(name) AS (VALUES (?)) SELECT * FROM foo"),
            "SELECT",
        )

    def test_with_values_newlines(self):
        self.assertEqual(
            pp.get_sql_command(
                "WITH foo(name) AS (VALUES (?), (?))\n      SELECT * FROM foo"
            ),
            "SELECT",
        )

    def test_with_values_multiple_columns(self):
        self.assertEqual(
            pp.get_sql_command(
                "WITH foo(name, id) AS (VALUES (?, ?), (?, ?)) SELECT * FROM foo"
            ),
            "SELECT",
        )

    def test_with_values_multiple_columns_newlines(self):
        self.assertEqual(
            pp.get_sql_command(
                "WITH\n foo\n(\nname\n,n id) \nAS \n(\nVALUES\n (\n?\n,\n ?\n)\n,\n (\n?\n,\n ?\n)\n)\n SELECT * FROM foo"
            ),
            "SELECT",
        )

    def test_with_explain_query_plan(self):
        self.assertEqual(
            pp.get_sql_command(
                "EXPLAIN QUERY PLAN WITH foo(name) AS (VALUES (?)) SELECT * FROM foo"
            ),
            "EXPLAIN",
        )


if __name__ == "__main__":
    unittest.main()
