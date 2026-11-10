LOCK TABLE model_pools, request_leases, admission_operations IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM model_pools WHERE (request_size_admission->>'Enabled')::boolean) THEN
        RAISE EXCEPTION 'disable request size admission before downgrading';
    END IF;
    IF EXISTS (SELECT 1 FROM request_leases WHERE reserved_tokens > 0 AND expires_at > clock_timestamp()) THEN
        RAISE EXCEPTION 'drain active token reservations before downgrading';
    END IF;
END $$;

ALTER TABLE admission_operations DROP COLUMN token_reservation;
ALTER TABLE request_leases DROP COLUMN reserved_tokens;
ALTER TABLE model_pools DROP COLUMN request_size_admission;
ALTER TABLE clients DROP COLUMN max_input_tokens, DROP COLUMN max_output_tokens,
    DROP COLUMN default_output_tokens, DROP COLUMN max_outstanding_tokens;
