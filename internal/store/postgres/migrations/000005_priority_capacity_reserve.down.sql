DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM model_pools WHERE high_priority_reserve <> 0) THEN
  RAISE EXCEPTION 'disable all priority reserves before rolling back';
 END IF;
END $$;
ALTER TABLE request_leases DROP COLUMN priority_class;
ALTER TABLE model_pools DROP COLUMN high_priority_reserve;
