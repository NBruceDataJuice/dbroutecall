CREATE TABLE dlq (
  id serial NOT NULL,
  ff_uuid uuid NOT NULL ,
  processor_group varchar(100), 
  processor varchar(100),
  attributes jsonb NOT NULL,
  insert_dt   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
