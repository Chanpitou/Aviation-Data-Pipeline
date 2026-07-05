SELECT
  COUNT(DISTINCT icao24)::NUMERIC AS on_ground_cnt
FROM raw_flight_logs
WHERE created_at > NOW() - INTERVAL '1 minutes'
  AND on_ground = true;