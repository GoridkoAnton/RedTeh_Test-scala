CREATE TABLE IF NOT EXISTS parquet_catalog (
  id SERIAL PRIMARY KEY,
  data_path TEXT NOT NULL,
  number_of_files INTEGER NOT NULL,
  average_files_size BIGINT NOT NULL,
  dt TIMESTAMP WITH TIME ZONE DEFAULT now()
);