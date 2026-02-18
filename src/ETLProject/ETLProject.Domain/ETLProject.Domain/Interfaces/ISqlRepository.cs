using ETLProject.Domain.Models;

namespace ETLProject.Domain.Interfaces;

public interface ISqlRepository
{
    Task BulkInsertTripsAsync(IEnumerable<CsvModel> trips);
}