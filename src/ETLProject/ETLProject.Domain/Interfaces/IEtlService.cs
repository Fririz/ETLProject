namespace ETLProject.Domain.Interfaces;

public interface IEtlService
{
    public Task RunImportAsync(string csvPath, string duplicatePath);
}