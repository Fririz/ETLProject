-- Find out which `PULocationId` (Pick-up location ID) has the highest tip_amo
SELECT TOP 1
    PULocationID,
    AVG(tip_amount) AS AverageTip
FROM Trips
GROUP BY PULocationID
ORDER BY AverageTip DESC;

-- Find the top 100 longest fares in terms of `trip_distance`.
SELECT TOP 100 *
FROM Trips
ORDER BY trip_distance DESC;


--- Find the top 100 longest fares in terms of time spent traveling.
SELECT TOP 100
    *,
    TripDurationSeconds AS DurationInSeconds,
    TripDurationSeconds / 60.0 AS DurationInMinutes
FROM Trips
ORDER BY TripDurationSeconds DESC;

--  - Search, where part of the conditions is `PULocationId`.
SELECT *
FROM Trips
WHERE PULocationID = 161
ORDER BY tpep_pickup_datetime;

--Total rows 29889
SELECT COUNT(*) AS TotalRowsCount FROM Trips;

-- Check Yes/No
SELECT DISTINCT store_and_fwd_flag
FROM Trips;

-- Check UTC
SELECT TOP 5 tpep_pickup_datetime FROM Trips;