using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.AspNetCore.SignalR;

namespace TwitchGqlMockServer;

public class ProxyHub : Hub
{
    private readonly ILogger<ProxyHub> _logger;

    public static readonly ConcurrentDictionary<string, bool> ConnectedClients = new();

    public ProxyHub(ILogger<ProxyHub> logger)
    {
        _logger = logger;
    }

    [HubMethodName("backend.communityTab")]
    public Task BackendCommunityTab(string response)
    {
        LogResponse("communityTab", response);
        return Task.CompletedTask;
    }

    [HubMethodName("backend.ViewerCards")]
    public Task BackendViewerCards(string response)
    {
        LogResponse("ViewerCards", response);
        return Task.CompletedTask;
    }

    [HubMethodName("ProxyShutdown")]
    public async Task OnProxyShutdown(JsonElement pendingOps)
    {
        var connectionId = Context.ConnectionId;
        _logger.LogWarning("Proxy shutting down: {Id}", connectionId);

        if (pendingOps.ValueKind != JsonValueKind.Array || pendingOps.GetArrayLength() == 0)
        {
            _logger.LogInformation("No pending operations to refire");
            return;
        }

        // Find another connected client (not the one shutting down)
        var otherClient = ConnectedClients.Keys.FirstOrDefault(id => id != connectionId);
        if (otherClient is null)
        {
            _logger.LogWarning("No other connected clients to refire operations to");
            return;
        }

        foreach (var op in pendingOps.EnumerateArray())
        {
            var channel = op.GetProperty("channel").GetString();
            if (channel is null) continue;

            _logger.LogInformation("Refiring operation on {Channel} to {TargetId}", channel, otherClient);
            await Clients.Client(otherClient).SendAsync(channel, op.GetProperty("payload"));
        }
    }

    private void LogResponse(string signalrChannel, string raw)
    {
        try
        {
            using var doc = JsonDocument.Parse(raw);
            var status = doc.RootElement.GetProperty("status").GetInt32();
            var twitchChannel = doc.RootElement.GetProperty("channel").GetString() ?? "?";
            _logger.LogInformation("✓ [{Channel}] status={Status} channel={TwitchChannel}", signalrChannel, status, twitchChannel);
        }
        catch
        {
            _logger.LogInformation("✓ [{Channel}] (raw): {Response}", signalrChannel, raw);
        }
    }

    public override Task OnConnectedAsync()
    {
        ConnectedClients.TryAdd(Context.ConnectionId, true);
        _logger.LogInformation("Client connected: {Id} (total: {Count})",
            Context.ConnectionId, ConnectedClients.Count);
        return base.OnConnectedAsync();
    }

    public override Task OnDisconnectedAsync(Exception? exception)
    {
        ConnectedClients.TryRemove(Context.ConnectionId, out _);
        _logger.LogInformation("Client disconnected: {Id} (remaining: {Count})",
            Context.ConnectionId, ConnectedClients.Count);
        return base.OnDisconnectedAsync(exception);
    }
}
