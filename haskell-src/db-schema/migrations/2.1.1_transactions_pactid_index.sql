CREATE INDEX pactIdIndex ON transactions using (pactid) where (pactid is not null);
-- maybe we could do the following:
-- CREATE INDEX pactIdIndex ON transactions using (height,pactid) where (pactid is not null);
