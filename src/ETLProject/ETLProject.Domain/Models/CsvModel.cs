using CsvHelper.Configuration.Attributes;

namespace ETLProject.Domain.Models;

public class CsvModel
{
    [Name("tpep_pickup_datetime")]
    public DateTime? PickupDatetime { get; set; }

    [Name("tpep_dropoff_datetime")]
    public DateTime? DropoffDatetime { get; set; }

    [Name("passenger_count")]
    public int? PassengerCount { get; set; }

    [Name("trip_distance")]
    public double? TripDistance { get; set; }

    [Name("store_and_fwd_flag")]
    public string? StoreAndFwdFlag { get; set; }

    [Name("PULocationID")]
    public int? PULocationId { get; set; }

    [Name("DOLocationID")]
    public int? DOLocationId { get; set; }

    [Name("fare_amount")]
    public decimal? FareAmount { get; set; }

    [Name("tip_amount")]
    public decimal? TipAmount { get; set; }
}