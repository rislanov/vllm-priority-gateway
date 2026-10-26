DO $$ BEGIN
 LOCK TABLE tokenization_operations,tokenization_leases IN ACCESS EXCLUSIVE MODE;
 IF EXISTS(SELECT 1 FROM tokenization_leases WHERE expires_at>clock_timestamp()) THEN
  RAISE EXCEPTION 'drain active tokenization leases before downgrade';
 END IF;
 DROP TABLE tokenization_leases,tokenization_operations,tokenization_rate_state,tokenization_backend_scopes,tokenization_client_scopes;
END $$;
