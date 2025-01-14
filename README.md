# prepper

## Install/update

```
cargo install --git https://github.com/beyondessential/prepper prepper
```

## Run

Enable replication in postgresql.conf:

```ini
wal_level = logical
```

Create a publication with the tables you want (or all tables):

```sql
CREATE PUBLICATION prepper WITH TABLE users;
```

Run prepper:

```
mkdir history
prepper --pg "postgres://user@localhost/database-name?pub=prepper" --out history
```

To start from scratch delete the output folder.

## File format

_Length-prefixed CBOR objects in rotated files._

Each file has the name `events-{device_id}-{start_time}.prp`:
- `{device_id}` is the Machine UUID in hex form with no hyphens.
- `{start_time}` is the timestamp of the first object in the file, in
  nanoseconds since the epoch, in base10.

This means that event files from multiple devices can be stored in the same
folder without ever colliding (and they'll group together via sorting).

Files are rotated:
- after a configurable length of time (default 1 hour);
- on program start.

This means that once a file is closed, it is not re-opened for writing. Only one
file is logically opened for writing at any given time (though there's a brief
period of time when a new file is created and the old one is getting flushed to).

The structure of the CBOR objects is described in the [schema.cddl](./schema.cddl)
in [CDDL](https://datatracker.ietf.org/doc/html/rfc8610) syntax.

Each CBOR object is prefixed with a 32-bit unsigned integer in CBOR form
(network-endian), which is the length of the serialised CBOR object in bytes.

### State

There's also a `_state.json` file. This is to be considered opaque as it contains
persisted state for the prepper process, but shouldn't be used by consumers, and
should only be modified by prepper.

## Known limitations

Most of these should be resolvable.

- Adding a table to a publication with ALTER doesn't stream the initial table data nor table schema to prepper until the next stream restart.
- Unsure yet how custom types are handled, need to revisit.
- Postgres arrays aren't supported yet and will crash prepper; this is easy but tedious to code.
- Nested postgres arrays aren't supported by upstream.
- Composite types aren't supported by upstream (or might be under custom types?).
- Output file size is ~3-5x the disk usage of postgres tables. However, compressing the files is highly effective, up to 0.1 ratios.
- The file format is a bit custom and not well handled by existing CBOR tooling, as there's no CBOR specification for streams of individual objects.
- There's no indexing of objects, and it's unclear yet what indexing would be useful.
- Transaction rollbacks aren't handled.
