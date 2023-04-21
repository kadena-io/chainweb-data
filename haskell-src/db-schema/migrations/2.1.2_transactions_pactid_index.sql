DROP INDEX IF EXISTS transactions_pactid_index;
CREATE INDEX transactions_pactid_index 
  ON transactions (pactid, goodresult DESC, height DESC)
  WHERE (goodresult IS NOT NULL AND pactid IS NOT NULL);
