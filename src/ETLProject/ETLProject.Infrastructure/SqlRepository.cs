using System.Data;
using ETLProject.Domain.Interfaces;
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

    public async Task BulkInsertTripsAsync(IEnumerable<DbModel> trips)
    {
        var table = new DataTable();
        

        table.Columns.Add(nameof(DbModel.PickupDatetime), typeof(DateTime));
        table.Columns.Add(nameof(DbModel.DropoffDatetime), typeof(DateTime));
        table.Columns.Add(nameof(DbModel.PassengerCount), typeof(int));
        table.Columns.Add(nameof(DbModel.TripDistance), typeof(double));
        table.Columns.Add(nameof(DbModel.StoreAndFwdFlag), typeof(string));
        table.Columns.Add(nameof(DbModel.PULocationId), typeof(int));
        table.Columns.Add(nameof(DbModel.DOLocationId), typeof(int));
        table.Columns.Add(nameof(DbModel.FareAmount), typeof(decimal));
        table.Columns.Add(nameof(DbModel.TipAmount), typeof(decimal));
        
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


        using var connection = new SqlConnection(_connectionString);
        await connection.OpenAsync();
        
        using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.TableLock, null);
        bulkCopy.DestinationTableName = "Trips";
        bulkCopy.BatchSize = 10000;
        bulkCopy.BulkCopyTimeout = 600;
        
        bulkCopy.ColumnMappings.Add(nameof(DbModel.PickupDatetime), "tpep_pickup_datetime");
        bulkCopy.ColumnMappings.Add(nameof(DbModel.DropoffDatetime), "tpep_dropoff_datetime");
        bulkCopy.ColumnMappings.Add(nameof(DbModel.PassengerCount), "passenger_count");
        bulkCopy.ColumnMappings.Add(nameof(DbModel.TripDistance), "trip_distance");
        bulkCopy.ColumnMappings.Add(nameof(DbModel.StoreAndFwdFlag), "store_and_fwd_flag");
        bulkCopy.ColumnMappings.Add(nameof(DbModel.PULocationId), "PULocationID");
        bulkCopy.ColumnMappings.Add(nameof(DbModel.DOLocationId), "DOLocationID");
        bulkCopy.ColumnMappings.Add(nameof(DbModel.FareAmount), "fare_amount");
        bulkCopy.ColumnMappings.Add(nameof(DbModel.TipAmount), "tip_amount");
        
        await bulkCopy.WriteToServerAsync(table);
    }
}