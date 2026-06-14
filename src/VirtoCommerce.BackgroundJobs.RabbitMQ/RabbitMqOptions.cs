namespace VirtoCommerce.BackgroundJobs.RabbitMQ;

/// <summary>
/// Connection settings for the RabbitMQ background-job engine.
/// Bound from configuration section <c>VirtoCommerce:BackgroundJobs:RabbitMQ</c> (provisional).
/// </summary>
public class RabbitMqOptions
{
    /// <summary>
    /// Full AMQP connection string, e.g. <c>amqp://guest:guest@localhost:5672/</c>.
    /// When set, it takes precedence over the individual host/port/credential properties below.
    /// </summary>
    public string? Uri { get; set; }

    public string HostName { get; set; } = "localhost";

    public int Port { get; set; } = 5672;

    public string UserName { get; set; } = "guest";

    public string Password { get; set; } = "guest";

    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// Friendly connection name shown in the RabbitMQ management UI.
    /// </summary>
    public string ClientProvidedName { get; set; } = "VirtoCommerce.BackgroundJobs";
}
