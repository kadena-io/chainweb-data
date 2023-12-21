# Changelog

## 2.4.0 (2023-12-21)

* Use CW-N's new block endpoint for history fill operations (#162)
* Fix treatment of events associated to multiple transactions returned by the `txs/txs` endpoint (#177)
* Update Haskell dependencies and GHC version (to 9.2.8) (#172, #174, #176)
* Nix build setup improvements (#170)
* Build with Nix and cache (to nixcache.chainweb.com) the project in GitHub actions (#165)
* README improvements (#168, #169, 178)
* Cabal build GitHub action fix (#166)
* Fix the hie.yaml for HLS (Haskell LSP) (#175, #179)

## 2.3.0 (2023-08-14)

* Fix `fill` and `backfill` issues caused by node's P2P API throttling, resulting in partial fills (#151)
* Decouple `chainweb-data` from the P2P API of `chainweb-node`, using the service endpoint exclusively instead. The `--p2p-{host,post}` CLI arguments are no longer needed, but are still recognized for backwards compatibility (#160)
* Enable running the HTTP API without the ETL, allowing read-only operation (#149)
* Attach signer and signature information to `TxDetail` endpoints (`/txs/{tx,txs}`) (#152,#153,#155)
* Extend network metadata to allow ETL'ing from (`fast-`)`development` nodes (#158)
* Add `--extra-migrations-folder` CLI argument (#163)

## 2.2.0 (2023-05-23)

This release completes the transition from `beam-automigrate` to the new incremental migration scripts (See Issue #101). If you're upgrading from a version earlier than 2.1.X, you'll first have to run CW-D v2.1.1 to migrate your database to a state that's compatible with the new script based migrations.

Note that after this version, the `transactionCount` field of the `/stats` endpoint becomes an estimate rather than an exact count. This change allowed us to decouple the ETL process from the HTTP server and fixed a long-standing resource accumulation issue.

* Remove the mutable server state and decouple the ETL from the HTTP server (#148)
* Enable searching by `pactid` in the `/txs/search` endpoint (#143)
* Include continuation transaction rows in the `/txs/search` resullt if their original transaction satisfies the search criteria (#66)
* Extend the `/txs/account` endpoint response with cross-chain information in the case of cross-chain transfers (#137, #138)
* Add `minheight`, `maxheight` query parameters to all transaction search enpoints (`/txs/{events,account,search}`) (#135)
* Add support for Nix flakes (#132, #133, #134, #136)
* Simplify the handling of the recentTxs (#140)
* Update maintainers list (#131)

## 2.1.1 (2023-01-23)

This is a quick release after 2.1.0 for fixing an oversight in the new `/txs/accounts` endpoint.
* Rename `chainid` -> `chain`, `name` -> `token` fields of `/txs/accounts` for consistency (#126)
* A new `minheight` parameter for `/txs/accounts` (#127)
* A `docker.nix` definition for building Ubuntu-based `chainweb-data` docker images using Nix (#84)

## 2.1.0 (2023-01-17)

_**IMPORTANT NOTICE**: Please skip this chainweb-data release and go straight to 2.1.1. Shortly after the release, we've noticed an oversight in the new `/txs/account` endpoint and decided to correct it with a quick breaking change instead of unnecessarily complicating the API. See [PR #126](https://github.com/kadena-io/chainweb-data/pull/126)._

This release drops the officiall support for Ubuntu 18.04 and adds support for Ubuntu 22.04 (see #100)

This is the last version that uses `beam-automigrate` for managing the database schema, from this version on, we'll switch to managing the schema using incremental migration scripts (see #101, #102, #104). When future versions of `chainweb-data` need to migrate the database from a version earlier than 2.1.0, they will ask the user to first run 2.1.0 to prepare their database for incremental migrations.

- A new `/txs/account` endpoint for fetching the incoming and outgoing transfers of a Kadena or non-Kadena account. #76 (also #83, #96, #103, #110, #114, #117, #124, #125)
- All search endpoints (`/txs/{account,events,search}`) now support an optional (at the discretion of the HTTP gateway) "bounded execution" workflow  (#109, also #118)
- The event search endpoint `/txs/event` now accepts 2 new arguments to narrow down the search results (#74):
   - `modulename`: Narrows down the search to events whose modules names match this value **exactly**
   - `minheight`: The minimum block height of the search window
- A _hidden_ new `--serve-swagger-ui` CLI argument that can be passed to `chainweb-data` to make it serve a Swagger UI for an auto-generated OpenAPI 3 spec for the `chainweb-data` HTTP API. The CLI argument is hidden because this spec is rudimentary and unofficial at the time of this release. Future releases will improve it.
- A new `--ignore-schema-diff` CLI argument to `chainweb-data` to make it ignore any unexpected database schema changes. This can be used by `chainweb-data` operators to make schema changes to their database and keep running  `chainweb-data`, but such ad-hoc database schema changes are not officially supported since they can cause a wide variety of errors under unpredictable conditions.
- A new `migrate` command for the `chainweb-data` CLI that can be used to run the database migrations and exit.
- A new `/txs/txs` endpoint similar to `/txs/tx`, but it returns a list of `TxDetail` objects, which can contain more than one entry when a transaction is introduced multiple times into the blockchain on independent branches. #71 #72
- Code search and event search query optimization (#67)
- Add requestkey indexes on `events` and `transactions` tables (#98)
- Refactor richlist generation (#89)
- Load-based throttling for search endpoints (#116)
- Optimize the recent transactions query at server start up (#119)
- Coin circulation calculation fix #97
- Set random_page_cost to 0 for CW-D connections #122


## 2.0.0 (2021-08-18)

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
