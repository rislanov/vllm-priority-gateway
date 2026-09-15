ALTER TABLE admission_operations
 DROP CONSTRAINT admission_concurrency_scope_reason,
 DROP COLUMN concurrency_scope;
