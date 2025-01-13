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
prepper --pg 'postgres://user@localhost/database-name?pub=prepper&slot=prepper' --out history
```

To start from scratch delete and recreate the output folder.

## File format

_Length-prefixed CBOR objects in rotated files._

Each file has the name `events-{device_id}-{start_time}.cbor`:
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

Files start with a length-prefixed header.

At the moment, the header is zero-sized, so each file starts with
`0x00 0x00 0x00 0x00`, but critically these shouldn't simply be skipped, but
read and then the corresponding amount of bytes skipped.

In the future, the header will contain indexes / lookup tables; reading the
header will never be required to understand the file.
