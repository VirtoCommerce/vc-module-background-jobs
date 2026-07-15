using Hangfire.MySql;
using Hangfire.PostgreSql;
using Hangfire.SqlServer;

namespace VirtoCommerce.Platform.Hangfire
{
    public class HangfireOptions
    {
        public HangfireJobStorageType JobStorageType { get; set; } = HangfireJobStorageType.Memory;

        /// <summary>
        /// Legacy Hangfire-only retry count. Used only as a fallback when the engine-agnostic
        /// <c>VirtoCommerce:BackgroundJobs:MaxRetryAttempts</c> is not explicitly configured; when that key is set it
        /// takes precedence (unifying Hangfire with the RabbitMQ engine).
        /// </summary>
        public int AutomaticRetryCount { get; set; } = 1;
        public int? WorkerCount { get; set; }
        public bool UseHangfireServer { get; set; } = true;
        public SqlServerStorageOptions SqlServerStorageOptions { get; set; } = new SqlServerStorageOptions();
        public MySqlStorageOptions MySqlStorageOptions { get; set; } = new MySqlStorageOptions();
        public PostgreSqlStorageOptions PostgreSqlStorageOptions { get; set; } = new PostgreSqlStorageOptions();
    }

    public enum HangfireJobStorageType
    {
        Memory,
        SqlServer,
        Database
    }
}
