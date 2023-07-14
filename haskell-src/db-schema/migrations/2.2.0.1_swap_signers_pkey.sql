ALTER TABLE signers DROP CONSTRAINT signers_pkey;
ALTER TABLE cwd.signers ADD CONSTRAINT signers_pkey PRIMARY KEY (requestkey, idx);
