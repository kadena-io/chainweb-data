CREATE INDEX transactions_pactid_index 
  ON transactions (pactid, (goodresult IS NOT NULL) DESC, height DESC)
  WHERE pactid IS NOT NULL;
