using System;
using System.Threading;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace VirtoCommerce.BackgroundJobs.RabbitMQ;

/// <summary>
/// Provides access to a shared, lazily-opened RabbitMQ <see cref="IConnection"/> and new channels.
/// </summary>
public interface IRabbitMqConnectionProvider : IAsyncDisposable
{
    /// <summary>
    /// Returns the shared connection, opening it on first use.
    /// </summary>
    Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Opens a new channel on the shared connection. The caller owns the channel and must dispose it.
    /// Pass <paramref name="options"/> (e.g. with publisher confirmations enabled) to control channel behavior.
    /// </summary>
    Task<IChannel> CreateChannelAsync(CreateChannelOptions? options = null, CancellationToken cancellationToken = default);
}
