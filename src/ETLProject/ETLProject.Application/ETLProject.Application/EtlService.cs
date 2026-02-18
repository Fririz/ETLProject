using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using ETLProject.Domain.Interfaces;
using ETLProject.Domain.Models;

namespace Application;

public class EtlService : IEtlService
{
    private readonly ISqlRepository _repository;
    private readonly ICsvService _csvService;
    private readonly TimeZoneInfo _estTimeZone = TimeZoneInfo.FindSystemTimeZoneById("America/New_York"); //because runs in linux container
    public EtlService(ISqlRepository repository, ICsvService csvService)
    {
        _repository = repository;
        _csvService = csvService;

    }
    //                         pickup    dropoff   passengers
    private readonly HashSet<(DateTime, DateTime, int)> _uniqueKeys = new();
    
    private string NormalizeFlag(string? flag)
    {
        var cleaned = flag?.Trim();
        return cleaned switch
        {
            "Y" => "Yes",
            "N" => "No",
            _ => "No"
        };
    }
    
    private DateTime ConvertToUtc(DateTime estTime)
    {
        return TimeZoneInfo.ConvertTimeToUtc(estTime, _estTimeZone);
    }
}