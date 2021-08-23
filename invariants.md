# Invariants that we expect in the mainnet DB

## Blocks

```
SELECT
  chainid,
  min(height) as min_height,
  max(height) as max_height,
  max(height) - min(height) as max_minus_min,
  sum(1) as count,
  sum(1) - max(height) + min(height) as num_orphans
FROM blocks
GROUP BY chainid;
```

The `count` column should be slightly larger than the `max_minus_min` column (due to orphans).

## Events

The events table should have slightly more than `max(transactions.height) - 1722501` coinbase rows (`requestkey = 'cb'`) when grouped by chainid (also due to orphans).

```
select chainid, sum(1) from events where requestkey = 'cb' group by chainid;
select chainid, max(height) - 1722501 from blocks group by chainid;
```


