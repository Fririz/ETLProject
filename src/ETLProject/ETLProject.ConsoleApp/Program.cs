using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Configuration;
using ETLProject.Domain.Interfaces;
using ETLProject.Infrastructure;
using Application;

var host = Host.CreateDefaultBuilder(args)
    .ConfigureServices((context, services) =>
    {
        var connectionString = context.Configuration.GetConnectionString("DefaultConnection") 
                               ?? throw new InvalidOperationException("ConnectionString not found.");

        services.AddScoped<ISqlRepository>(_ => new SqlRepository(connectionString));
        services.AddScoped<ICsvService, CsvService>();
        services.AddScoped<IEtlService, EtlService>();
    })
    .Build();

try
{
    using var scope = host.Services.CreateScope();
    var config = scope.ServiceProvider.GetRequiredService<IConfiguration>();
    var etlService = scope.ServiceProvider.GetRequiredService<IEtlService>();

    string input = config["FilePaths:InputCsv"] ?? "data/sample-cab-data.csv";
    string output = config["FilePaths:DuplicatesCsv"] ?? "data/duplicates.csv";
        
    var outDir = Path.GetDirectoryName(output);
    if (!string.IsNullOrEmpty(outDir)) Directory.CreateDirectory(outDir);

    if (!File.Exists(input)) throw new FileNotFoundException($"File not found: {input}");

    Console.WriteLine($"Starting Import");
    
    await etlService.RunImportAsync(input, output);

    Console.WriteLine("Done");
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
}