using ETLProject.Domain.Interfaces;

namespace ETLProject.Infrastructure;

public class SqlRepository : ISqlRepository
{
    private readonly string _connectionString;
    
    public SqlRepository(string connectionString)
    {
        _connectionString = connectionString;
    }
    
    
}