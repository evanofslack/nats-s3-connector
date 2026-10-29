ALTER TABLE chunks DROP CONSTRAINT chunks_bucket_key_unique;

UPDATE chunks SET prefix = NULL WHERE prefix = '';

-- keys were stored with the prefix prepended, make them relative to prefix
UPDATE chunks
SET key = substr(key, length(prefix) + 2)
WHERE prefix IS NOT NULL
  AND left(key, length(prefix) + 1) = prefix || '/';

-- null prefixes were never unique, keep only the latest row per location
DELETE FROM chunks a
USING chunks b
WHERE a.bucket = b.bucket
  AND a.prefix IS NOT DISTINCT FROM b.prefix
  AND a.key = b.key
  AND a.sequence_number < b.sequence_number;

ALTER TABLE chunks
    ADD CONSTRAINT chunks_bucket_prefix_key_unique UNIQUE NULLS NOT DISTINCT (bucket, prefix, key);
