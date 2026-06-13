using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Virtocommerce.Backgroundjobs.Data.Repositories;

namespace Virtocommerce.Backgroundjobs.Data.PostgreSql;

public class DesignTimeDbContextFactory : IDesignTimeDbContextFactory<BackgroundjobsDbContext>
{
    public BackgroundjobsDbContext CreateDbContext(string[] args)
    {
        var builder = new DbContextOptionsBuilder<BackgroundjobsDbContext>();
        var connectionString = args.Length != 0 ? args[0] : "Server=localhost;Username=virto;Password=virto;Database=VirtoCommerce3;";

        builder.UseNpgsql(
            connectionString,
            options => options.MigrationsAssembly(typeof(PostgreSqlDataAssemblyMarker).Assembly.GetName().Name));

        return new BackgroundjobsDbContext(builder.Options);
    }
}
