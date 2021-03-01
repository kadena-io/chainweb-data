# Changelog

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
