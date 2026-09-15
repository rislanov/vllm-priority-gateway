ALTER TABLE admission_operations
 ADD COLUMN concurrency_scope TEXT NOT NULL DEFAULT ''
 CHECK (concurrency_scope IN ('', 'client', 'pool')),
 ADD CONSTRAINT admission_concurrency_scope_reason
 CHECK (concurrency_scope = '' OR rejection_reason = 'concurrency_exhausted');
