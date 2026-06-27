using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR.Client;
using Microsoft.Extensions.Logging;

namespace TwitchGqlProxy;

public sealed class ProxyService : IAsyncDisposable
{
    private readonly ILogger<ProxyService> _logger;
    private readonly HttpClient _httpClient;
    private readonly HubConnection _hubConnection;
    private readonly ProxyConfig _config;

    private readonly ConcurrentQueue<BatchItem> _queue = new();
    private readonly object _flushLock = new();
    private int _pendingCount;
    private Timer? _debounceTimer;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = false,
    };

    public ProxyService(ILogger<ProxyService> logger, HttpClient httpClient, ProxyConfig config)
    {
        _logger = logger;
        _httpClient = httpClient;
        _config = config;

        _httpClient.DefaultRequestHeaders.TryAddWithoutValidation("Client-Id", config.ClientId);

        _hubConnection = new HubConnectionBuilder()
            .WithUrl(config.SignalrHubUrl)
            .WithAutomaticReconnect()
            .Build();

        _hubConnection.Reconnecting += _ =>
        {
            _logger.LogWarning("SignalR connection lost, reconnecting...");
            return Task.CompletedTask;
        };

        _hubConnection.Reconnected += _ =>
        {
            _logger.LogInformation("SignalR reconnected");
            return Task.CompletedTask;
        };

        _hubConnection.Closed += ex =>
        {
            _logger.LogError(ex, "SignalR connection closed permanently");
            return Task.CompletedTask;
        };

        foreach (var channel in config.Channels)
        {
            var channelCopy = channel;
            _hubConnection.On(channelCopy, (JsonElement payload) =>
            {
                HandleIncoming(channelCopy, payload);
            });
        }
    }

    public async Task StartAsync(CancellationToken ct)
    {
        _logger.LogInformation("Connecting to SignalR hub at {Url}", _config.SignalrHubUrl);
        await _hubConnection.StartAsync(ct);
        _logger.LogInformation("Connected to SignalR hub, listening on channels: {Channels}",
            string.Join(", ", _config.Channels));
    }

    public async Task StopAsync()
    {
        List<BatchItem>? pendingBatch;
        lock (_flushLock)
        {
            _debounceTimer?.Dispose();
            _debounceTimer = null;
            pendingBatch = DrainQueue();
        }
        if (pendingBatch is { Count: > 0 })
        {
            await FlushBatchAsync(pendingBatch);
        }
        await _hubConnection.StopAsync();
    }

    private void HandleIncoming(string signalrChannel, JsonElement payload)
    {
        var operationName = payload.GetProperty("operation").GetString()
            ?? throw new InvalidOperationException("Missing 'operation' in incoming payload");
        var twitchChannel = payload.GetProperty("channel").GetString()
            ?? throw new InvalidOperationException("Missing 'channel' in incoming payload");
        var username = payload.TryGetProperty("username", out var u) ? u.GetString() : null;

        JsonElement operation;

        switch (operationName)
        {
            case "CommunityTab":
                using (var doc = JsonDocument.Parse(JsonSerializer.Serialize(new
                {
                    operationName = "CommunityTab",
                    variables = new { login = twitchChannel },
                    extensions = new
                    {
                        persistedQuery = new
                        {
                            version = 1,
                            sha256Hash = "92168b4434c8f4d32df14510052131c3544b929723d5f8b69bb96c96207e483e",
                        },
                    },
                })))
                {
                    operation = doc.RootElement.Clone();
                }

                _ = FlushBatchAsync([new BatchItem(signalrChannel, twitchChannel, operation)]);
                _logger.LogInformation("  → CommunityTab [{Channel}] flushed immediately", twitchChannel);
                return;

            case "ViewerCard":
                using (var doc = JsonDocument.Parse(JsonSerializer.Serialize(new
                {
                    operationName = "ViewerCard",
                    variables = new
                    {
                        channelLogin = twitchChannel,
                        hasChannelID = false,
                        giftRecipientLogin = username ?? twitchChannel,
                        isViewerBadgeCollectionEnabled = true,
                        withStandardGifting = false,
                        badgeSourceChannelLogin = twitchChannel,
                    },
                    extensions = new
                    {
                        persistedQuery = new
                        {
                            version = 1,
                            sha256Hash = "80c53fe04c79a6414484104ea573c28d6a8436e031a235fc6908de63f51c74fd",
                        },
                    },
                })))
                {
                    operation = doc.RootElement.Clone();
                }
                break;

            default:
                throw new InvalidOperationException($"Unknown operation: {operationName}");
        }

        // ViewerCard: batch up to 20 per request
        var item = new BatchItem(signalrChannel, twitchChannel, operation);
        List<BatchItem>? batchToFlush = null;

        lock (_flushLock)
        {
            _queue.Enqueue(item);
            _pendingCount++;
            _logger.LogInformation("  + ViewerCard [{Channel}] queued ({Pending}/20)", twitchChannel, _pendingCount);

            if (_pendingCount >= _config.MaxBatchSize)
            {
                batchToFlush = DrainQueue();
                _logger.LogInformation("  → Batch full, flushing {Count} ops", batchToFlush.Count);
            }
        }

        if (batchToFlush is { Count: > 0 })
        {
            _ = FlushBatchAsync(batchToFlush);
        }
    }

    private List<BatchItem> DrainQueue()
    {
        if (_pendingCount == 0) return [];

        var items = new List<BatchItem>(_pendingCount);
        while (_queue.TryDequeue(out var item))
        {
            items.Add(item);
        }
        _pendingCount = 0;
        return items;
    }

    private async Task FlushBatchAsync(List<BatchItem> batch)
    {
        var stopwatch = ValueStopwatch.StartNew();

        try
        {
            var requestArray = new JsonElement[batch.Count];
            for (int i = 0; i < batch.Count; i++)
            {
                requestArray[i] = batch[i].Payload;
            }

            var requestBody = JsonSerializer.Serialize(requestArray, JsonOptions);
            var content = new StringContent(requestBody, Encoding.UTF8, "application/json");

            await EnforceRateLimit();

            var response = await _httpClient.PostAsync(_config.GqlEndpoint, content);
            var responseBody = await response.Content.ReadAsStringAsync();

            if (!response.IsSuccessStatusCode)
            {
                _logger.LogError("GQL returned {StatusCode}: {Body}", (int)response.StatusCode, responseBody);

                foreach (var item in batch)
                {
                    await SendErrorResponse(item, (int)response.StatusCode, responseBody);
                }
                return;
            }

            JsonDocument responseDoc;
            try
            {
                responseDoc = JsonDocument.Parse(responseBody);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "Failed to parse GQL response JSON");
                foreach (var item in batch)
                {
                    await SendErrorResponse(item, 502, responseBody);
                }
                return;
            }

            using (responseDoc)
            {
                if (responseDoc.RootElement.ValueKind == JsonValueKind.Array)
                {
                    var responses = responseDoc.RootElement.EnumerateArray().ToArray();

                    if (responses.Length != batch.Count)
                    {
                        _logger.LogWarning(
                            "Batch size mismatch: sent {Sent}, got {Received}",
                            batch.Count, responses.Length);
                    }

                    int count = Math.Min(responses.Length, batch.Count);
                    for (int i = 0; i < count; i++)
                    {
                        var wrapped = JsonSerializer.Serialize(new
                        {
                            status = 200,
                            channel = batch[i].ResponseChannel,
                            body = responses[i],
                        });
                        await SendResponse(batch[i].Channel, wrapped);
                    }

                    for (int i = count; i < batch.Count; i++)
                    {
                        await SendErrorResponse(batch[i], 502, "No response received for this operation");
                    }
                }
                else
                {
                    _logger.LogWarning("GQL response was not an array");
                    foreach (var item in batch)
                    {
                        var wrapped = JsonSerializer.Serialize(new
                        {
                            status = 200,
                            channel = item.ResponseChannel,
                            body = responseDoc.RootElement,
                        });
                        await SendResponse(item.Channel, wrapped);
                    }
                }
            }

            var elapsed = stopwatch.Elapsed;
            _logger.LogInformation(
                "✓ Flushed {Count} ops to GQL in {ElapsedMs}ms (status={StatusCode})",
                batch.Count, elapsed.TotalMilliseconds, (int)response.StatusCode);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Failed to flush batch of {Count} operations", batch.Count);
            foreach (var item in batch)
            {
                try { await SendErrorResponse(item, 500, ex.Message); }
                catch { }
            }
        }
    }

    private async Task SendResponse(string channel, string jsonPayload)
    {
        try
        {
            if (_hubConnection.State == HubConnectionState.Connected)
            {
                await _hubConnection.InvokeAsync(channel, jsonPayload);
            }
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "Failed to send response on channel {Channel}", channel);
        }
    }

    private async Task SendErrorResponse(BatchItem item, int status, string body)
    {
        var json = JsonSerializer.Serialize(new
        {
            status,
            channel = item.ResponseChannel,
            body,
        });
        await SendResponse(item.Channel, json);
    }

    private readonly Queue<DateTime> _rateLimitTimestamps = new();
    private readonly object _rateLimitLock = new();

    private async Task EnforceRateLimit()
    {
        if (_config.RateLimitPerMinute <= 0)
            return;

        while (true)
        {
            DateTime delayUntil = DateTime.MinValue;
            lock (_rateLimitLock)
            {
                var now = DateTime.UtcNow;
                var cutoff = now.AddMinutes(-1);

                while (_rateLimitTimestamps.Count > 0 && _rateLimitTimestamps.Peek() < cutoff)
                {
                    _rateLimitTimestamps.Dequeue();
                }

                if (_rateLimitTimestamps.Count < _config.RateLimitPerMinute)
                {
                    _rateLimitTimestamps.Enqueue(now);
                    return;
                }

                var oldest = _rateLimitTimestamps.Peek();
                delayUntil = oldest.AddMinutes(1);
            }

            var delay = delayUntil - DateTime.UtcNow;
            if (delay > TimeSpan.Zero)
            {
                _logger.LogWarning("Rate limit reached, delaying {DelayMs}ms", delay.TotalMilliseconds);
                await Task.Delay(delay);
            }
        }
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _httpClient.Dispose();
        await _hubConnection.DisposeAsync();
        _debounceTimer?.Dispose();
    }

    private sealed record BatchItem(string Channel, string ResponseChannel, JsonElement Payload);
}

public sealed record ProxyConfig
{
    public string SignalrHubUrl { get; init; } = "http://localhost:5000/hub";
    public string GqlEndpoint { get; init; } = "https://gql.twitch.tv/gql";
    public string ClientId { get; init; } = "";
    public string[] Channels { get; init; } = ["backend.communityTab", "backend.UserCards"];
    public int MinBatchSize { get; init; } = 5;
    public int MaxBatchSize { get; init; } = 20;
    public int DebounceMs { get; init; } = 300;
    public int RateLimitPerMinute { get; init; } = 5000;

    public static ProxyConfig FromEnvironment()
    {
        return new ProxyConfig
        {
            SignalrHubUrl = Environment.GetEnvironmentVariable("SIGNALR_HUB_URL")
                ?? "http://localhost:5000/hub",
            GqlEndpoint = Environment.GetEnvironmentVariable("GQL_ENDPOINT")
                ?? "https://gql.twitch.tv/gql",
            ClientId = Environment.GetEnvironmentVariable("CLIENT_ID")
                ?? throw new InvalidOperationException("CLIENT_ID environment variable is required"),
            Channels = Environment.GetEnvironmentVariable("CHANNELS")
                ?.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries)
                ?? ["backend.communityTab", "backend.UserCards"],
            MinBatchSize = int.TryParse(
                Environment.GetEnvironmentVariable("MIN_BATCH_SIZE"), out var min) ? min : 5,
            MaxBatchSize = int.TryParse(
                Environment.GetEnvironmentVariable("MAX_BATCH_SIZE"), out var max) ? max : 20,
            DebounceMs = int.TryParse(
                Environment.GetEnvironmentVariable("DEBOUNCE_MS"), out var dms) ? dms : 300,
            RateLimitPerMinute = int.TryParse(
                Environment.GetEnvironmentVariable("RATE_LIMIT"), out var rl) ? rl : 5000,
        };
    }
}

internal struct ValueStopwatch
{
    private readonly long _startTimestamp;

    private ValueStopwatch(long startTimestamp)
    {
        _startTimestamp = startTimestamp;
    }

    public static ValueStopwatch StartNew() => new(Stopwatch.GetTimestamp());

    public TimeSpan Elapsed
    {
        get
        {
            var elapsed = Stopwatch.GetTimestamp() - _startTimestamp;
            return TimeSpan.FromTicks(elapsed * TimeSpan.TicksPerSecond / Stopwatch.Frequency);
        }
    }
}
