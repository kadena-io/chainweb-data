# Changelog

## 2.0.0 (pending)

This is a major backwards-incompatible update. All chainweb-data users need to
delete their DB and rebuild from scratch.  Major changes include:

- DB fills in several hours instead of several days
- New command `fill` replaces the old `backfill` and `gaps` commands
- The `server` command now has a `-f` option which will run `fill` once a day to
  fill any gaps in the block data
- New tables for events and transaction signers
- Standardized to storing base64url values with no padding (i.e. the '=' at the end)
- Fixed a bug where parent block hashes were stored instead of the block hash

## 1.3.0 (2021-03-31)

Adds `events` table and provides the following new queries/endpoints:
  - `tx` which is a "tx detail" search by request key returning a single result, with full events
  - `events` which is an event search supporting the following params (along with limit and offset):
    - `param` for param text wildcard search
    - `requestkey` for exact request key search
    - `name` for event name wildcard search (e.g. "TRANSFER")
    - `idx` for exact event idx search (meaningless without `requestKey` but oh well)


## 1.2.0 (2021-02-20)

#### Changed

- Added support for chainweb-node's new separated P2P and Service APIs being served on separate ports.
- Added ability to rate limit the `gaps` and `backfill` commands with a
  `--delay` commandline option.

## 1.1.0 (2020-03-05)

#### Changed

- The `transactions` table now stores the entire `PayloadWithOutputs` structure,
  not just the smaller `Payload`.

## 1.0.0 (2020-03-03)

Initial release.
