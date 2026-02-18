IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'EtlDb')
BEGIN
    CREATE DATABASE EtlDb;
END
GO

USE EtlDb;
GO

IF OBJECT_ID('dbo.Trips', 'U') IS NOT NULL
    DROP TABLE dbo.Trips;
GO

CREATE TABLE Trips (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,
    tpep_pickup_datetime DATETIME2(0) NOT NULL,
    tpep_dropoff_datetime DATETIME2(0) NOT NULL,
    passenger_count INT,
    trip_distance FLOAT,
    store_and_fwd_flag VARCHAR(3),
    PULocationID INT,
    DOLocationID INT,
    fare_amount DECIMAL(10, 2),
    tip_amount DECIMAL(10, 2),
    
--To "find faster top 100 longest fares in terms of time spent traveling" 
    TripDurationSeconds AS DATEDIFF(second, tpep_pickup_datetime, tpep_dropoff_datetime) PERSISTED
);
GO
