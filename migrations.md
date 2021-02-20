# 2021-02-19 Integer size migration

Old type was `integer`

```
ALTER TABLE blocks
ALTER COLUMN chainid SET DATA TYPE bigint,
ALTER COLUMN height SET DATA TYPE bigint;

ALTER TABLE transactions
ALTER COLUMN chainid SET DATA TYPE bigint,
ALTER COLUMN ttl SET DATA TYPE bigint,
ALTER COLUMN gaslimit SET DATA TYPE bigint,
ALTER COLUMN step SET DATA TYPE bigint,
ALTER COLUMN gas SET DATA TYPE bigint,
ALTER COLUMN txid SET DATA TYPE bigint;
```
