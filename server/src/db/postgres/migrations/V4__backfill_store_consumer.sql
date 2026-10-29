UPDATE store_jobs SET consumer = regexp_replace(subject, '[.>*]', '_', 'g') WHERE consumer IS NULL;
