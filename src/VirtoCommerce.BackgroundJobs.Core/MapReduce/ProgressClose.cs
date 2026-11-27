using System;
using VirtoCommerce.Platform.Core.Jobs;

namespace VirtoCommerce.BackgroundJobs.Core.MapReduce;

/// <summary>What closed a batch's progress notification, kept in the batch store so a map task can send it again.</summary>
public sealed record ProgressClose(string Description, JobOutcome Outcome, DateTime Finished);
