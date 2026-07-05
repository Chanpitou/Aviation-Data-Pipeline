SELECT DISTINCT ON(icao24)
    icao24,
    callsign,
    origin_country,
    longitude,
    latitude,
    altitude,
    velocity,
    created_at
FROM raw_flight_logs
WHERE created_at > NOW() - INTERVAL '1 minutes'
   AND on_ground = false;
