CREATE TABLE IF NOT EXISTS public.spark_compact_metadata (
  id               BIGSERIAL PRIMARY KEY,
  data_path        TEXT NOT NULL,
  number_of_files  INTEGER NOT NULL,
  average_file_mb  NUMERIC(12,3) NOT NULL,
  created_at_utc   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
