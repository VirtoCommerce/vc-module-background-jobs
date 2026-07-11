#nullable enable
using System;
using VirtoCommerce.BackgroundJobs.RabbitMQ;
using Xunit;

namespace VirtoCommerce.BackgroundJobs.Tests;

/// <summary>
/// Unit tests for <see cref="RabbitMqOptions"/> concurrency resolution. Prefetch and dispatch concurrency auto-scale
/// to the CPU count when left at their default 0 (or any value &lt;= 0); a positive value pins them explicitly. Pure
/// option logic — no broker required.
/// </summary>
public class RabbitMqOptionsTests
{
    [Fact]
    public void Default_Prefetch_AutoScales_With_Cpu()
    {
        // 0 is the default => auto-scale from ProcessorCount.
        var options = new RabbitMqOptions { ConcurrencyPerCore = 4, MaxAutoConcurrency = 1000 };

        var expected = (ushort)Math.Clamp(Environment.ProcessorCount * 4, 1, 1000);
        Assert.Equal(expected, options.EffectivePrefetchCount());
        // Dispatch follows the effective prefetch when it too is left at 0.
        Assert.Equal(expected, options.EffectiveDispatchConcurrency());
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(-100)]
    public void NonPositive_Prefetch_AutoScales(int prefetch)
    {
        var options = new RabbitMqOptions { PrefetchCount = prefetch, ConcurrencyPerCore = 4, MaxAutoConcurrency = 1000 };

        var expected = (ushort)Math.Clamp(Environment.ProcessorCount * 4, 1, 1000);
        Assert.Equal(expected, options.EffectivePrefetchCount());
    }

    [Fact]
    public void Explicit_Prefetch_Wins_And_Is_Not_Clamped_By_MaxAuto()
    {
        // A positive value is honored as-is; MaxAutoConcurrency only bounds the auto-derived value.
        var options = new RabbitMqOptions { PrefetchCount = 64, ConcurrencyPerCore = 10, MaxAutoConcurrency = 20 };

        Assert.Equal((ushort)64, options.EffectivePrefetchCount());
    }

    [Fact]
    public void Auto_Prefetch_Clamps_To_Max()
    {
        var options = new RabbitMqOptions { ConcurrencyPerCore = 100_000, MaxAutoConcurrency = 50 };

        Assert.Equal((ushort)50, options.EffectivePrefetchCount());
    }

    [Fact]
    public void Explicit_Dispatch_Concurrency_Overrides()
    {
        var options = new RabbitMqOptions { ConsumerDispatchConcurrency = 12, ConcurrencyPerCore = 10 };

        Assert.Equal((ushort)12, options.EffectiveDispatchConcurrency());
    }

    [Fact]
    public void Explicit_Prefetch_Drives_Auto_Dispatch()
    {
        // Prefetch pinned, dispatch left at 0 => dispatch follows the explicit prefetch.
        var options = new RabbitMqOptions { PrefetchCount = 30 };

        Assert.Equal((ushort)30, options.EffectiveDispatchConcurrency());
    }
}
