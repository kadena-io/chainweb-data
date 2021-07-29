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

# 2021-07-29 1.3.0 Schema migration

* Add `num_events` to transactions table
* Expand transactions primary key

```
BEGIN;
ALTER TABLE transactions ADD COLUMN num_events SET DATA TYPE bigint;
ALTER TABLE transactions DROP CONSTRAINT transactions_pkey;
ALTER TABLE transactions ADD PRIMARY KEY (requestkey, block);
COMMIT;
```

* A new `events` table is also being added but that component of the migration
  will be performed automatically if you run with `-m`.

