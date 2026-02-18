Number of rows: 29889

Number of duplicates: 444

## How to run:
1. Create .env at src/ETLProject

example:

```env
MSSQL_PASSWORD=SUd34E0ETVoQ

DB_NAME=EtlDb

SQL_PORT=1433
```
2. Start the database
docker-compose up -d db

3. Connect to Database (at localhost:{SQL_PORT}) and execute scripts from src/ETLProject/Sql:

4. Place your input .csv file in src/ETLProject/data/.
Note: If the filename differs from the default, update it in appsettings.json.

5. Run program
docker-compose run etlproject

6. Check result(all scripts for checking at /src/ETLProject/Sql/TestSqlQueries.sql)


## Key Features
1. Uses SqlBulkCopy for high-speed insertion of records.
2. Utilizes IAsyncEnumerable and streaming to process data row by row without loading the entire file into memory.
3. Converts timestamps from EST to UTC, supporting both Windows and Linux environments.
4. Clean Architecture

## Scalability

The current implementation stores all unique keys in RAM using a HashSet. For a 10GB file, this would crash the application with an OutOfMemoryException. To fix this, I would change the strategy to one of the following:

1. Instead of checking for duplicates in C#, I would bulk insert all data including duplicates into a temporary "staging" table in SQL Server. Then, I would run a SQL query to move only unique rows to the main table. SQL Server is much better optimized for handling massive datasets than a C# application's memory.
2. I would sort the CSV file on disk first. Once the file is sorted, duplicate records will be next to each other. This allows me to check for duplicates by simply comparing the current row with the previous one, using almost no RAM.

## CI/CD Pipeline
1. Restores dependencies to check for package consistency
2. Builds the solution to verify compilation
3. Runs Unit Tests to ensure no logic is broken


## Testing
The project includes Unit Tests
Run tests using:

dotnet test

