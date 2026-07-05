-- Current in-air aircraft count
SELECT
    COUNT(DISTINCT icao24) AS aircraft_count
FROM raw_flight_logs
WHERE created_at > NOW() - INTERVAL '1 minutes'
    AND on_ground = false;