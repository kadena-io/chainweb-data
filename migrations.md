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
# 2021-06-25 Events schema change


```
ALTER TABLE transactions
ADD COLUMN num_events SET DATA TYPE bigint;

ALTER TABLE events
RENAME COLUMN requestkey TO sourcekey,
ADD COLUMN sourcetype SET DATA TYPE VARCHAR SET NOT NULL,
DROP CONSTRAINT events_pkey,
ADD PRIMARY KEY (sourcekey, idx);

UPDATE events SET sourcetype = 'Source_Tx';
```

# 2021-07-16 Expand transactions primary key

```
BEGIN;
ALTER TABLE transactions DROP CONSTRAINT transactions_pkey;
ALTER TABLE transactions ADD PRIMARY KEY (requestkey, block);
COMMIT;
```
