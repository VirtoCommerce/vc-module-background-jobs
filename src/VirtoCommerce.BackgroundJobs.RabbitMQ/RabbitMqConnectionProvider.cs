using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace VirtoCommerce.BackgroundJobs.RabbitMQ;

/// <summary>
/// Default <see cref="IRabbitMqConnectionProvider"/>: keeps a single shared connection, opened lazily and
/// re-opened if it drops. Uses the RabbitMQ.Client 7.x asynchronous API.
/// </summary>
public sealed class RabbitMqConnectionProvider : IRabbitMqConnectionProvider
{
    private readonly RabbitMqOptions _options;
    private readonly ILogger<RabbitMqConnectionProvider> _logger;
    private readonly SemaphoreSlim _connectionLock = new(1, 1);
    private IConnection? _connection;

    public RabbitMqConnectionProvider(IOptions<RabbitMqOptions> options, ILogger<RabbitMqConnectionProvider> logger)
    {
        _options = options.Value;
        _logger = logger;
    }

    public async Task<IConnection> GetConnectionAsync(CancellationToken cancellationToken = default)
    {
        if (_connection is { IsOpen: true })
        {
            return _connection;
        }

        await _connectionLock.WaitAsync(cancellationToken);
        try
        {
            if (_connection is { IsOpen: true })
            {
                return _connection;
            }

            if (_connection is not null)
            {
                await _connection.DisposeAsync();
                _connection = null;
            }

            var factory = CreateConnectionFactory();
            _logger.LogInformation("Opening RabbitMQ connection '{ClientName}'", _options.ClientProvidedName);
            _connection = await factory.CreateConnectionAsync(cancellationToken);
            return _connection;
        }
        finally
        {
            _connectionLock.Release();
        }
    }

    public async Task<IChannel> CreateChannelAsync(CreateChannelOptions? options = null, CancellationToken cancellationToken = default)
    {
        var connection = await GetConnectionAsync(cancellationToken);
        return await connection.CreateChannelAsync(options, cancellationToken);
    }

    private ConnectionFactory CreateConnectionFactory()
    {
        var factory = new ConnectionFactory
        {
            ClientProvidedName = _options.ClientProvidedName,
        };

        if (!string.IsNullOrWhiteSpace(_options.Uri))
        {
            factory.Uri = new Uri(_options.Uri);
        }
        else
        {
            factory.HostName = _options.HostName;
            factory.Port = _options.Port;
            factory.UserName = _options.UserName;
            factory.Password = _options.Password;
            factory.VirtualHost = _options.VirtualHost;
        }

        return factory;
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            await _connection.DisposeAsync();
            _connection = null;
        }

        _connectionLock.Dispose();
    }
}
