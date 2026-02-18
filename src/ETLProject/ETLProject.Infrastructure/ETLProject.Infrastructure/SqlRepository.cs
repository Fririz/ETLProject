using ETLProject.Domain.Interfaces;
using System.Data;
using ETLProject.Domain.Models;
using Microsoft.Data.SqlClient;


namespace ETLProject.Infrastructure;

public class SqlRepository : ISqlRepository
{
    private readonly string _connectionString;
    
    public SqlRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    public async Task BulkInsertTripsAsync(IEnumerable<CsvModel> trips)
    {
        var table = new DataTable();
        table.Columns.Add("PickupDatetime", typeof(DateTime));
        table.Columns.Add("DropoffDatetime", typeof(DateTime));
        table.Columns.Add("PassengerCount", typeof(int));
        table.Columns.Add("TripDistance", typeof(double));
        table.Columns.Add("StoreAndFwdFlag", typeof(string));
        table.Columns.Add("PULocationId", typeof(int));
        table.Columns.Add("DOLocationId", typeof(int));
        table.Columns.Add("FareAmount", typeof(decimal));
        table.Columns.Add("TipAmount", typeof(decimal));
        
        foreach (var trip in trips)
        {
            table.Rows.Add(
                trip.PickupDatetime,
                trip.DropoffDatetime,
                trip.PassengerCount,
                trip.TripDistance,
                trip.StoreAndFwdFlag,
                trip.PULocationId,
                trip.DOLocationId,
                trip.FareAmount,
                trip.TipAmount
            );
        }

        using var bulkCopy = new SqlBulkCopy(_connectionString, SqlBulkCopyOptions.TableLock);
        bulkCopy.DestinationTableName = "Trips";
        bulkCopy.BatchSize = 10000;
        bulkCopy.BulkCopyTimeout = 600;

        bulkCopy.ColumnMappings.Add("PickupDatetime", "tpep_pickup_datetime");
        bulkCopy.ColumnMappings.Add("DropoffDatetime", "tpep_dropoff_datetime");
        bulkCopy.ColumnMappings.Add("PassengerCount", "passenger_count");
        bulkCopy.ColumnMappings.Add("TripDistance", "trip_distance");
        bulkCopy.ColumnMappings.Add("StoreAndFwdFlag", "store_and_fwd_flag");
        bulkCopy.ColumnMappings.Add("PULocationId", "PULocationID");
        bulkCopy.ColumnMappings.Add("DOLocationId", "DOLocationID");
        bulkCopy.ColumnMappings.Add("FareAmount", "fare_amount");
        bulkCopy.ColumnMappings.Add("TipAmount", "tip_amount");
        
        await bulkCopy.WriteToServerAsync(table);
    }
}