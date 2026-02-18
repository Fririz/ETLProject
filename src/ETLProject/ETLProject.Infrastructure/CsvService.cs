using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using ETLProject.Domain.Interfaces;
using ETLProject.Domain.Models;

namespace ETLProject.Infrastructure;

public class CsvService : ICsvService
{
    public async IAsyncEnumerable<CsvModel> ReadTripsAsync(string filePath)
    {
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            MissingFieldFound = null,
            HeaderValidated = null,
            ReadingExceptionOccurred = (r) => 
            {
                return false;
            },
        };
        
        using var reader = new StreamReader(filePath);
        using var csv = new CsvReader(reader, config);

        await foreach (var record in csv.GetRecordsAsync<CsvModel>())
        {
            yield return record;
        }
    }
    public async Task WriteDuplicateAsync(string filePath, IEnumerable<CsvModel> records)
    {
        var csvModels = records.ToList();
        
        if (!csvModels.Any()) return;
        var config = new CsvConfiguration(CultureInfo.InvariantCulture)
        {
            HasHeaderRecord = !File.Exists(filePath) 
        };
        
        using var stream = new FileStream(filePath, FileMode.Append, FileAccess.Write, FileShare.Read);
        using var writer = new StreamWriter(stream);
        using var csv = new CsvWriter(writer, config);

        await csv.WriteRecordsAsync(csvModels);
    }
}