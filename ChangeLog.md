# Changelog

## Unreleased

#### Changed

- `backfill` will not start unless there is at least one block in the DB for
  each chain.
- `backfill` shows a much more accurate progress report.

#### Fixed

- `backfill` should no longer fail to fetch payloads when the queried node is
  under strain.

## 1.1.0 (2020-03-05)

#### Changed

- The `transactions` table now stores the entire `PayloadWithOutputs` structure,
  not just the smaller `Payload`.

## 1.0.0 (2020-03-03)

Initial release.
