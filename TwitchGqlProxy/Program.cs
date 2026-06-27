using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using TwitchGqlProxy;

var config = ProxyConfig.FromEnvironment();

var services = new ServiceCollection()
    .AddLogging(builder =>
    {
        builder.AddConsole();
        builder.SetMinimumLevel(
            Enum.TryParse<LogLevel>(
                Environment.GetEnvironmentVariable("LOG_LEVEL"), ignoreCase: true, out var level)
            ? level : LogLevel.Information);
    })
    .AddSingleton(config)
    .AddHttpClient<ProxyService>(client =>
    {
        client.Timeout = TimeSpan.FromSeconds(30);
    })
    .Services;

var provider = services.BuildServiceProvider();
var logger = provider.GetRequiredService<ILogger<Program>>();
var proxy = provider.GetRequiredService<ProxyService>();

var shutdownTcs = new TaskCompletionSource();
Console.CancelKeyPress += (_, args) =>
{
    args.Cancel = true;
    logger.LogInformation("Shutting down...");
    shutdownTcs.TrySetResult();
};

try
{
    logger.LogInformation(
        "Starting Twitch GQL Batch Proxy (min={Min}, max={Max}, debounce={Dms}ms, rateLimit={Rl}/min)",
        config.MinBatchSize, config.MaxBatchSize, config.DebounceMs, config.RateLimitPerMinute);

    await proxy.StartAsync(CancellationToken.None);
    logger.LogInformation("Proxy is running. Press Ctrl+C to stop.");

    await shutdownTcs.Task;
    await proxy.StopAsync();
}
catch (OperationCanceledException) { }
catch (Exception ex)
{
    logger.LogCritical(ex, "Proxy failed");
    return 1;
}
finally
{
    await proxy.DisposeAsync();
}

return 0;
