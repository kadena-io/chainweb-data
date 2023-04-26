DROP INDEX IF EXISTS transactions_pactid_index;
CREATE INDEX transactions_pactid_index 
  ON transactions (pact_id, goodresult DESC, height DESC) 
  WHERE pactid IS NOT NULL;
