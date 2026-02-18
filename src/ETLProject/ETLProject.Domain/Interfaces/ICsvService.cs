using ETLProject.Domain.Models;

namespace ETLProject.Domain.Interfaces;

public interface ICsvService
{
    public IAsyncEnumerable<CsvModel> ReadTripsAsync(string filePath);
    public Task WriteDuplicateAsync(string filePath, IEnumerable<CsvModel> records);
}