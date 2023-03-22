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


if __name__ == "__main__":
    unittest.main()
