(tiled-authn-database)=
# Set up a database for a scaled authenticated deployment

When Tiled is configured to use authentication provider(s), it employs a SQL
database to store authentication-related state, such as identities, sessions,
and API keys. In single-process deployments, it creates a SQLite database
automatically at startup. For scaled deployments, a proper scalable database
should be used. Tiled supports any SQL dialect support by
[SQLAlchemy](https://www.sqlalchemy.org/), but we recommend PostgreSQL for
scaled deployments.

## Create a database and user for tiled

Invent a strong password, such as via:

    openssl rand -hex 32

or

    python -c "import secrets; print(secrets.token_hex(32))"

Create a user and a database in PostgreSQL. For example:

```
$ sudo su postgres
$ psql
postgres=# CREATE USER 'tiled' WITH SUPERUSER PASSWORD '...';
postgres=# CREATE DATABASE tiled ENCODING 'utf-8' OWNER tiled;
```

## Configure and initialize the database

Place the database URI in the configuration file, filling in the hostname
in place of `...` below. Inject the password via an environment variable as shown;
do not hard-code it in the configuration file. Be sure to quote the URI.

```yaml
database
  uri: "postgresql://tiled:${TILED_DATABASE_PASSWORD}@.../tiled"
```

Initialize the database. Initialization only has to be done once ever. (If you
run this on an existing database Tiled will notice and refuse to initialize it.)

```
$ tiled admin initialize-database postgresql://tiled:${TILED_DATABASE_PASSWORD}@.../tiled
```

The database is ready to use.

## Reference

See `database` in {doc}`../reference/service-configuration` for comprehensive
documentation of the options for tuning database performance and reliability.
