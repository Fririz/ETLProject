using System.Runtime.InteropServices;
using ETLProject.Domain.Interfaces;
using ETLProject.Domain.Models;

namespace Application;

public class EtlService : IEtlService
{
    private readonly ISqlRepository _repository;
    private readonly ICsvService _csvService;

    private readonly TimeZoneInfo _estTimeZone;
    public EtlService(ISqlRepository repository, ICsvService csvService)
    {
        _repository = repository;
        _csvService = csvService;
        
        string timeZoneId = RuntimeInformation.IsOSPlatform(OSPlatform.Windows)
            ? "Eastern Standard Time" // Windows
            : "America/New_York";     // Linux

        try
        {
            _estTimeZone = TimeZoneInfo.FindSystemTimeZoneById(timeZoneId);
        }
        catch (TimeZoneNotFoundException)
        {
            throw new Exception($"Timezone error: {timeZoneId}");
        }
    }
    //                         pickup    dropoff   passengers
    private readonly HashSet<(DateTime, DateTime, int)> _uniqueKeys = new();
    
    
    public async Task RunImportAsync(string csvPath, string duplicatePath)
    {
        const int batchSize = 5000;
        
        var validBatch = new List<DbModel>(batchSize);
        
        var duplicatesBatch = new List<CsvModel>(batchSize);

        await foreach (var record in _csvService.ReadTripsAsync(csvPath))
        {
            var key = (
                record.PickupDatetime.GetValueOrDefault(),
                record.DropoffDatetime.GetValueOrDefault(),
                record.PassengerCount.GetValueOrDefault()
            );
            
            if (_uniqueKeys.Contains(key))
            {
                duplicatesBatch.Add(record);

                if (duplicatesBatch.Count >= batchSize)
                {
                    await _csvService.WriteDuplicateAsync(duplicatePath, duplicatesBatch);
                    duplicatesBatch.Clear();
                }
            }
            else
            {
                _uniqueKeys.Add(key);

                var dbModel = MapToDbModel(record);

                if (dbModel != null)
                {
                    validBatch.Add(dbModel);
                }

                if (validBatch.Count >= batchSize)
                {
                    await _repository.BulkInsertTripsAsync(validBatch);
                    validBatch.Clear();
                }
            }
        }
        
        if (validBatch.Any())
        {
            await _repository.BulkInsertTripsAsync(validBatch);
        }

        if (duplicatesBatch.Any())
        {
            await _csvService.WriteDuplicateAsync(duplicatePath, duplicatesBatch);
        }
    }
    
    private DbModel? MapToDbModel(CsvModel source)
    {
        if (source.PickupDatetime == null || source.DropoffDatetime == null)
        {
            return null;
        }

        return new DbModel
        {
            PickupDatetime = ConvertToUtc(source.PickupDatetime.Value),
            DropoffDatetime = ConvertToUtc(source.DropoffDatetime.Value),
            
            PassengerCount = source.PassengerCount.GetValueOrDefault(0),
            TripDistance = source.TripDistance.GetValueOrDefault(0),
            
            StoreAndFwdFlag = NormalizeFlag(source.StoreAndFwdFlag),
            
            PULocationId = source.PULocationId.GetValueOrDefault(0),
            DOLocationId = source.DOLocationId.GetValueOrDefault(0),
            FareAmount = source.FareAmount.GetValueOrDefault(0),
            TipAmount = source.TipAmount.GetValueOrDefault(0)
        };
    }
    
    private string NormalizeFlag(string? flag)
    {
        var cleaned = flag?.Trim();
        return cleaned switch
        {
            "Y" => "Yes",
            _ => "No"
        };
    }
    
    private DateTime ConvertToUtc(DateTime estTime)
    {
        return TimeZoneInfo.ConvertTimeToUtc(estTime, _estTimeZone);
    }
}