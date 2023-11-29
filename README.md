# rqdb

This is an unofficial python client for [rqlite](https://github.com/rqlite/rqlite), a
lightweight distributed relational database based on SQLite.

This client supports SQLite syntax, true parameterized queries, and a
calling syntax reminiscent of DB API 2.0.

Furthermore, this client has convenient asynchronous methods which match
the underlying rqlite API.

## Installation

```py
pip install rqdb
```

## Usage

Synchronous queries:

```py
import rqdb
import secrets

conn = rqdb.connect(['127.0.0.1:4001'])
cursor = conn.cursor()
cursor.execute('CREATE TABLE persons (id INTEGER PRIMARY KEY, uid TEXT UNIQUE NOT NULL, name TEXT NOT NULL)')
cursor.execute('CREATE TABLE pets (id INTEGER PRIMARY KEY, name TEXT NOT NULL, owner_id INTEGER NOT NULL REFERENCES persons(id) ON DELETE CASCADE)')

# standard execute
cursor.execute('INSERT INTO persons (uid, name) VALUES (?, ?)', (secrets.token_hex(8), 'Jane Doe'))
assert cursor.rows_affected == 1

# The following is stored in a single Raft entry and executed within a transaction.

person_name = 'John Doe'
person_uid = secrets.token_urlsafe(16)
pet_name = 'Fido'
result = cursor.executemany3((
    (
        'INSERT INTO persons (uid, name) VALUES (?, ?)',
        (person_uid, person_name)
    ),
    (
        'INSERT INTO pets (name, owner_id) '
        'SELECT'
        '  ?, persons.id '
        'FROM persons '
        'WHERE uid = ?',
        (pet_name, person_uid)
    )
)).raise_on_error()
assert result[0].rows_affected == 1
assert result[1].rows_affected == 1
```

Asynchronous queries:

```py
import rqdb
import secrets

async def main():
    async with rqdb.connect_async(['127.0.0.1:4001']) as conn:
        cursor = conn.cursor()

        result = await cursor.execute(
            'INSERT INTO persons (uid, name) VALUES (?, ?)',
            (secrets.token_hex(8), 'Jane Doe')
        )
        assert result.rows_affected == 1
```

## Additional Features

### Explain

Quickly get a formatted query plan from the current leader for a query:

```py
import rqdb

conn = rqdb.connect(['127.0.0.1:4001'])
cursor = conn.cursor()
cursor.execute('CREATE TABLE persons (id INTEGER PRIMARY KEY, uid TEXT UNIQUE NOT NULL, given_name TEXT NOT NULL, family_name TEXT NOT NULL)')
cursor.explain("SELECT id FROM persons WHERE TRIM(given_name || ' ' || family_name) LIKE ?", ('john d%',), out='print')
# --SCAN persons
cursor.execute("CREATE INDEX persons_name_idx ON persons(TRIM(given_name || ' ' || family_name) COLLATE NOCASE)")
cursor.explain("SELECT id FROM persons WHERE TRIM(given_name || ' ' || family_name) LIKE ?", ('john d%',), out='print')
# --SEARCH persons USING INDEX persons_name_idx (<expr>>? AND <expr><?)
```

### Read Consistency

Selecting read consistency is done at the cursor level, either by passing
`read_consistency` to the cursor constructor (`conn.cursor()`) or by setting
the instance variable `read_consistency` directly. The available consistencies
are `strong`, `weak`, and `none`. You may also indicate the `freshness` value
at the cursor level.

See [CONSISTENCY.md](https://github.com/rqlite/rqlite/blob/master/DOC/CONSISTENCY.md) for
details.

The default consistency is `weak`.

### Foreign Keys

Foreign key support in rqlite is disabled by default, to match sqlite. This is a common source
of confusion. It cannot be configured by the client reliably. Foreign key support
is enabled as described in
[FOREIGN_KEY_CONSTRAINTS.md](https://github.com/rqlite/rqlite/blob/master/DOC/FOREIGN_KEY_CONSTRAINTS.md)

### Nulls

Substituting "NULL" in parametrized queries can be error-prone. In particular,
sqlite needs null sent in a very particular way, which the rqlite server has
historically not handled properly.

By default, if you attempt to use "None" as a parameter to a query, this package
will perform string substition with the value "NULL" in the correct spot. Be
careful however - you will still need to handle nulls properly in the query,
since "col = NULL" and "col IS NULL" are not the same. In particular, `NULL = NULL`
is `NULL`, which evaluates to false. One way this could be handled is

```py
name: Optional[str] = None

# never matches a row since name is None, even if the rows name is null
cursor.execute('SELECT * FROM persons WHERE name = ?', (name,))

# works as expected
cursor.execute('SELECT * FROM persons WHERE ((? IS NULL AND name IS NULL) OR name = ?)', (name, name))
```

### Backup

Backups can be initiated using `conn.backup(filepath: str, raw: bool = False)`.
The download will be streamed to the given filepath. Both the sql format and a
compressed sqlite format are supported.

### Logging

By default this will log using the standard `logging` module. This can be disabled
using `log=False` in the `connect` call. If logging is desired but just needs to be
configured slightly, it can be done as follows:

```py
import rqdb
import logging

conn = rqdb.connect(
    ['127.0.0.1:4001'],
    log=rqdb.LogConfig(
        # Started a SELECT query
        read_start={
            'enabled': True,
            'level': logging.DEBUG,  # alternatively, 'method': logging.debug
        },

        # Started a UPDATE/INSERT query
        write_start={
            'enabled': True,
            'level': logging.DEBUG,
        },

        # Got the response from the database for a SELECT query
        read_response={
            'enabled': True,
            'level': logging.DEBUG,,
            'max_length': 1024,  # limits how much of the response we log
        },

        # Got the response from the database for a UPDATE/INSERT query
        write_response={
            'enabled': True,
            'level': logging.DEBUG,
        },

        # Failed to connect to one of the nodes.
        connect_timeout={
            'enabled': True,
            'level': logging.WARNING,
        },

        # Failed to connect to any node for a query
        hosts_exhausted={
            'enabled': True,
            'level': logging.CRITICAL,
        },

        # The node returned a status code other than 200-299 or
        # a redirect when a redirect is allowed.
        non_ok_response={
            'enabled': True,
            'level': logging.WARNING
        }
    )
)
```

## Limitations

### Slow Transactions

The primary limitations is that by the connectionless nature of rqlite, while
transactions are possible, the entire transaction must be specified upfront.
That is, you cannot open a transaction, perform a query, and then use the
result of that query to perform another query before closing the transaction.

This can also be seen as a blessing, as these types of transactions are the most
common source of performance issues in traditional applications. They require
long-held locks that can easily lead to N^2 performance. The same behavior can
almost always be achieved with uids, as shown in the example. The repeated UID
lookup causes a consistent overhead, which is highly preferable to the
unpredictable negative feedback loop nature of long transactions.

## Other Notes

It is often helpful to combine this library with a sql builder such
as [pypika](https://pypika.readthedocs.io/en/latest/) when manipulating
complex queries.
