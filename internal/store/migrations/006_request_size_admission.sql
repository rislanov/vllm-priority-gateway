ALTER TABLE clients ADD COLUMN max_input_tokens INTEGER NOT NULL DEFAULT 0 CHECK (max_input_tokens BETWEEN 0 AND 1000000000000);
ALTER TABLE clients ADD COLUMN max_output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (max_output_tokens BETWEEN 0 AND 1000000000000);
ALTER TABLE clients ADD COLUMN default_output_tokens INTEGER NOT NULL DEFAULT 0 CHECK (default_output_tokens BETWEEN 0 AND 1000000000000);
ALTER TABLE clients ADD COLUMN max_outstanding_tokens INTEGER NOT NULL DEFAULT 0 CHECK (max_outstanding_tokens BETWEEN 0 AND 1000000000000);
ALTER TABLE model_pools ADD COLUMN request_size_admission TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(request_size_admission) AND json_type(request_size_admission) = 'object');
