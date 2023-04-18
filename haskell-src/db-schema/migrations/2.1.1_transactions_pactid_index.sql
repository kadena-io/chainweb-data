CREATE INDEX transactions_pactid_idx
  ON transactions (pactid)
  WHERE (pactid IS NOT NULL);