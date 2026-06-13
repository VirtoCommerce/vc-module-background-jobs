using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using VirtoCommerce.BackgroundJobs.Data.Repositories;

namespace VirtoCommerce.BackgroundJobs.Data.PostgreSql;

public class DesignTimeDbContextFactory : IDesignTimeDbContextFactory<BackgroundJobsDbContext>
{
    public BackgroundJobsDbContext CreateDbContext(string[] args)
    {
        var builder = new DbContextOptionsBuilder<BackgroundJobsDbContext>();
        var connectionString = args.Length != 0 ? args[0] : "Server=localhost;Username=virto;Password=virto;Database=VirtoCommerce3;";

        builder.UseNpgsql(
            connectionString,
            options => options.MigrationsAssembly(typeof(PostgreSqlDataAssemblyMarker).Assembly.GetName().Name));

        return new BackgroundJobsDbContext(builder.Options);
    }
}
