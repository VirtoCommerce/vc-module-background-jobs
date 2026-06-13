using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Design;
using Virtocommerce.Backgroundjobs.Data.Repositories;

namespace Virtocommerce.Backgroundjobs.Data.MySql;

public class DesignTimeDbContextFactory : IDesignTimeDbContextFactory<BackgroundjobsDbContext>
{
    public BackgroundjobsDbContext CreateDbContext(string[] args)
    {
        var builder = new DbContextOptionsBuilder<BackgroundjobsDbContext>();
        var connectionString = args.Length != 0 ? args[0] : "Server=localhost;User=virto;Password=virto;Database=VirtoCommerce3;";

        builder.UseMySql(
            connectionString,
            ResolveServerVersion(args, connectionString),
            options => options.MigrationsAssembly(typeof(MySqlDataAssemblyMarker).Assembly.GetName().Name));

        return new BackgroundjobsDbContext(builder.Options);
    }

    private static ServerVersion ResolveServerVersion(string[] args, string connectionString)
    {
        var serverVersion = args.Length >= 2 ? args[1] : null;

        if (serverVersion == "AutoDetect")
        {
            return ServerVersion.AutoDetect(connectionString);
        }

        if (serverVersion != null)
        {
            return ServerVersion.Parse(serverVersion);
        }

        return new MySqlServerVersion(new Version(5, 7));
    }
}
