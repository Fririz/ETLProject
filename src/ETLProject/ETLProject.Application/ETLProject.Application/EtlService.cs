using System.Globalization;
using CsvHelper;
using CsvHelper.Configuration;
using ETLProject.Domain.Interfaces;

namespace Application;

public class EtlService : IEtlService
{
    private readonly ISqlRepository _repository;
    private readonly ICsvService _csvService;
    public EtlService(ISqlRepository repository, ICsvService csvService)
    {
        _repository = repository;
        _csvService = csvService;
    }
}