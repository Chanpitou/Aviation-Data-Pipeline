-- Altitude histogram across current captured in-air flights
SELECT
  FLOOR(altitude / 1000) * 1000 AS altitude_bin,
  COUNT(*) AS flight_count
FROM raw_flight_logs
WHERE created_at > NOW() - INTERVAL '1 minutes'
  AND on_ground = false
  AND altitude IS NOT NULL
GROUP BY 1
ORDER BY 1;