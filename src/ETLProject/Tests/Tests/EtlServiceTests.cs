using Application;
using ETLProject.Domain.Interfaces;
using ETLProject.Domain.Models;
using Moq;

namespace Tests;

public class EtlServiceTests
{
    private readonly Mock<ISqlRepository> _repositoryMock;
    private readonly Mock<ICsvService> _csvServiceMock;
    private readonly EtlService _service;

    public EtlServiceTests()
    {
        _repositoryMock = new Mock<ISqlRepository>();
        _csvServiceMock = new Mock<ICsvService>();
        
        _service = new EtlService(_repositoryMock.Object, _csvServiceMock.Object);
    }

    [Fact]
    public async Task RunImportAsync_Should_CallRepository_When_DataIsValid()
    {
        var csvPath = "input.csv";
        var dupPath = "dups.csv";
        var records = new List<CsvModel>
        {
            new() { 
                PickupDatetime = new DateTime(2023, 1, 1, 10, 0, 0), 
                DropoffDatetime = new DateTime(2023, 1, 1, 10, 30, 0),
                PassengerCount = 1,
                StoreAndFwdFlag = "Y"
            }
        };

        _csvServiceMock.Setup(x => x.ReadTripsAsync(csvPath))
            .Returns(CreateAsyncEnumerable(records));

        await _service.RunImportAsync(csvPath, dupPath);

        _repositoryMock.Verify(x => x.BulkInsertTripsAsync(It.Is<IEnumerable<DbModel>>(
            list => list.Any(item => item.StoreAndFwdFlag == "Yes")
        )), Times.Once);
    }

    [Fact]
    public async Task RunImportAsync_Should_IdentifyAndWriteDuplicates_ToCsv()
    {
        var csvPath = "input.csv";
        var dupPath = "dups.csv";
        
        var records = new List<CsvModel>
        {
            new() { PickupDatetime = new DateTime(2023, 1, 1), DropoffDatetime = new DateTime(2023, 1, 2), PassengerCount = 2 },
            new() { PickupDatetime = new DateTime(2023, 1, 1), DropoffDatetime = new DateTime(2023, 1, 2), PassengerCount = 2 }
        };

        _csvServiceMock.Setup(x => x.ReadTripsAsync(csvPath))
            .Returns(CreateAsyncEnumerable(records));

        await _service.RunImportAsync(csvPath, dupPath);


        _repositoryMock.Verify(x => x.BulkInsertTripsAsync(It.Is<IEnumerable<DbModel>>(l => l.Count() == 1)), Times.Once);
        
        _csvServiceMock.Verify(x => x.WriteDuplicateAsync(dupPath, It.Is<List<CsvModel>>(l => l.Count == 1)), Times.Once);
    }

    [Fact]
    public async Task RunImportAsync_Should_SkipRecords_With_MissingDates()
    {
        var records = new List<CsvModel>
        {
            new() { PickupDatetime = null, DropoffDatetime = new DateTime(2023, 1, 1) }
        };

        _csvServiceMock.Setup(x => x.ReadTripsAsync(It.IsAny<string>()))
            .Returns(CreateAsyncEnumerable(records));

        await _service.RunImportAsync("in.csv", "out.csv");

        _repositoryMock.Verify(x => x.BulkInsertTripsAsync(It.IsAny<IEnumerable<DbModel>>()), Times.Never);
    }

    [Theory]
    [InlineData("Y", "Yes")]
    [InlineData("N", "No")]
    [InlineData("  Y  ", "Yes")]
    [InlineData(null, "No")]
    [InlineData("Random", "No")]
    public async Task RunImportAsync_Should_CorrectlyNormalizeFlags(string inputFlag, string expected)
    {
        var records = new List<CsvModel>
        {
            new() { 
                PickupDatetime = DateTime.Now, 
                DropoffDatetime = DateTime.Now.AddHours(1), 
                StoreAndFwdFlag = inputFlag 
            }
        };

        _csvServiceMock.Setup(x => x.ReadTripsAsync(It.IsAny<string>()))
            .Returns(CreateAsyncEnumerable(records));

        await _service.RunImportAsync("in.csv", "out.csv");

        _repositoryMock.Verify(x => x.BulkInsertTripsAsync(It.Is<IEnumerable<DbModel>>(
            list => list.First().StoreAndFwdFlag == expected
        )), Times.Once);
    }

    private static async IAsyncEnumerable<T> CreateAsyncEnumerable<T>(IEnumerable<T> items)
    {
        foreach (var item in items)
        {
            yield return item;
        }
        await Task.CompletedTask;
    }
}