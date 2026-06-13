using System.Reflection;
using Microsoft.EntityFrameworkCore;
//using VirtoCommerce.Platform.Data.Extensions;
using VirtoCommerce.Platform.Data.Infrastructure;

namespace Virtocommerce.Backgroundjobs.Data.Repositories;

public class BackgroundjobsDbContext : DbContextBase
{
    public BackgroundjobsDbContext(DbContextOptions<BackgroundjobsDbContext> options)
        : base(options)
    {
    }

    protected BackgroundjobsDbContext(DbContextOptions options)
        : base(options)
    {
    }

    protected override void OnModelCreating(ModelBuilder modelBuilder)
    {
        base.OnModelCreating(modelBuilder);

        //modelBuilder.Entity<BazQuxEntity>().ToAuditableEntityTable("BazQux");

        switch (Database.ProviderName)
        {
            case "Pomelo.EntityFrameworkCore.MySql":
                modelBuilder.ApplyConfigurationsFromAssembly(Assembly.Load("Virtocommerce.Backgroundjobs.Data.MySql"));
                break;
            case "Npgsql.EntityFrameworkCore.PostgreSQL":
                modelBuilder.ApplyConfigurationsFromAssembly(Assembly.Load("Virtocommerce.Backgroundjobs.Data.PostgreSql"));
                break;
            case "Microsoft.EntityFrameworkCore.SqlServer":
                modelBuilder.ApplyConfigurationsFromAssembly(Assembly.Load("Virtocommerce.Backgroundjobs.Data.SqlServer"));
                break;
        }
    }
}
