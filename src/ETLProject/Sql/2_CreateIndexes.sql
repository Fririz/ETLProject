DROP INDEX IF EXISTS IX_Trips_PULocationID_Include_Tip ON Trips;
--for "Find out which PULocationId has the highest tip_amount on average"
CREATE NONCLUSTERED INDEX IX_Trips_PULocationID_Include_Tip 
ON Trips(PULocationID) 
INCLUDE (tip_amount);
GO

-- for "Find the top 100 longest fares in terms of trip_distance."
DROP INDEX IF EXISTS IX_Trips_Distance ON Trips;
CREATE NONCLUSTERED INDEX IX_Trips_Distance 
ON Trips(trip_distance DESC);
GO

-- for "find faster top 100 longest fares in terms of time spent traveling"
DROP INDEX IF EXISTS IX_Trips_Duration ON Trips;
CREATE NONCLUSTERED INDEX IX_Trips_Duration 
ON Trips(TripDurationSeconds DESC);
GO