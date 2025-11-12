#!/usr/bin/env bash
set -euo pipefail

# default driver memory if not provided
: "${SPARK_DRIVER_MEMORY:=6g}"

# If first arg looks like spark-submit (user explicitly provided), just exec it
if [ "$1" = "spark-submit" ]; then
  exec "$@"
fi

# Run spark-submit with the application jar and forward all args to the app
exec /opt/spark/bin/spark-submit \
  --master local[4] \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --class com.example.SmallFilesAndCompact \
  /app/job.jar "$@"