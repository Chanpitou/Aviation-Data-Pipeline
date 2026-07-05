#
SELECT
    origin_country,
    COUNT(DISTINCT icao24) AS aircraft_count
FROM raw_flight_logs
WHERE created_at > NOW() - INTERVAL '1 minutes'
    AND on_ground = false
GROUP BY origin_country
ORDER BY aircraft_count DESC;