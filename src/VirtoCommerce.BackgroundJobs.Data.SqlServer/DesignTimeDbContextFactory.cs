using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using VirtoCommerce.BackgroundJobs.Data.Repositories;

namespace VirtoCommerce.BackgroundJobs.Data.SqlServer;

public class DesignTimeDbContextFactory : IDesignTimeDbContextFactory<BackgroundJobsDbContext>
{
    public BackgroundJobsDbContext CreateDbContext(string[] args)
    {
        var builder = new DbContextOptionsBuilder<BackgroundJobsDbContext>();
        var connectionString = args.Length != 0 ? args[0] : "Server=(local);User=virto;Password=virto;Database=VirtoCommerce3;";

        builder.UseSqlServer(
            connectionString,
            options => options.MigrationsAssembly(typeof(SqlServerDataAssemblyMarker).Assembly.GetName().Name));

        return new BackgroundJobsDbContext(builder.Options);
    }
}
