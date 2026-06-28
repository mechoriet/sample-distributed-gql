using Microsoft.AspNetCore.SignalR;

namespace TwitchGqlMockServer;

public class TestOperationService : BackgroundService
{
    private readonly IHubContext<ProxyHub> _hubContext;
    private readonly ILogger<TestOperationService> _logger;

    public TestOperationService(IHubContext<ProxyHub> hubContext, ILogger<TestOperationService> logger)
    {
        _hubContext = hubContext;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Mock server ready. Commands:");
        _logger.LogInformation("  ct <channel>          — Send CommunityTab to a random connected proxy");
        _logger.LogInformation("  uc <channel>          — Send ViewerCards to a random connected proxy");
        _logger.LogInformation("  batch <n> [channel]   — Send n ViewerCards ops (default: forsen)");
        _logger.LogInformation("  top [n]               — Send CommunityTab for top n streamers (default: 100)");
        _logger.LogInformation("  q                     — Quit");
        _logger.LogInformation("");

        while (!stoppingToken.IsCancellationRequested)
        {
            var line = await Console.In.ReadLineAsync(stoppingToken);
            if (line is null) break;
            line = line.Trim();
            if (line == "q") break;

            try
            {
                await ProcessCommand(line, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Command failed");
            }
        }
    }

    private async Task ProcessCommand(string line, CancellationToken ct)
    {
        var parts = line.Split(' ', StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0) return;

        var command = parts[0].ToLowerInvariant();

        switch (command)
        {
            case "ct":
            {
                var channel = parts.Length > 1 ? parts[1] : "testchannel";
                await SendCommunityTab(channel, ct);
                break;
            }

            case "uc":
            {
                var channel = parts.Length > 1 ? parts[1] : "testchannel";
                await SendViewerCards(channel, ct);
                break;
            }

            case "batch":
            {
                var count = parts.Length > 1 && int.TryParse(parts[1], out var n) ? n : 10;
                var baseChannel = parts.Length > 2 ? parts[2] : "forsen";
                for (int i = 0; i < count; i++)
                {
                    await SendViewerCardsRaw($"user{i}", baseChannel, ct);
                }
                _logger.LogInformation("Sent {Count} ViewerCards operations (channelLogin={Login})",
                    count, baseChannel);
                break;
            }

            case "top":
            {
                var count = parts.Length > 1 && int.TryParse(parts[1], out var t) ? t : 100;
                count = Math.Min(count, TopStreamers.Length);
                _logger.LogInformation("Sending CommunityTab for top {Count} streamers...", count);
                for (int i = 0; i < count; i++)
                {
                    await SendCommunityTab(TopStreamers[i], ct);
                    if (i % 10 == 9)
                        _logger.LogInformation("  ... {Pct}% done", (i + 1) * 100 / count);
                }
                _logger.LogInformation("Sent CommunityTab for {Count} streamers", count);
                break;
            }

            default:
                _logger.LogWarning("Unknown command: {Cmd}", command);
                break;
        }
    }

    private async Task SendViewerCardsRaw(string username, string channelLogin, CancellationToken ct)
    {
        var clientId = PickRandomClient();
        if (clientId is null) return;

        await _hubContext.Clients.Client(clientId).SendAsync("backend.ViewerCards", new
        {
            operation = "ViewerCard",
            channel = channelLogin,
            username,
        }, ct);

        _logger.LogInformation("→ backend.ViewerCards channel={Channel} username={User} to client {Id}",
            channelLogin, username, clientId);
    }

    private string? PickRandomClient()
    {
        var clients = ProxyHub.ConnectedClients.Keys;
        if (clients.Count == 0)
        {
            _logger.LogWarning("No connected clients to send to");
            return null;
        }
        var index = Random.Shared.Next(clients.Count);
        return clients.ElementAt(index);
    }

    private async Task SendCommunityTab(string channelName, CancellationToken ct)
    {
        var clientId = PickRandomClient();
        if (clientId is null) return;

        await _hubContext.Clients.Client(clientId).SendAsync("backend.communityTab", new
        {
            operation = "CommunityTab",
            channel = channelName,
            username = (string?)null,
        }, ct);

        _logger.LogInformation("→ backend.communityTab channel={Channel} to client {Id}", channelName, clientId);
    }

    private async Task SendViewerCards(string channelName, CancellationToken ct)
    {
        await SendViewerCardsRaw(channelName, channelName, ct);
    }

    private static readonly string[] TopStreamers =
    [
        "xqc", "summit1g", "lirik", "shroud", "ninja",
        "timthetatman", "drdisrespect", "sodapoppin", "asmongold", "forsen",
        "pokimane", "valkyrae", "sykkuno", "ludwig", "moistcr1tikal",
        "hannahxr0se", "kyedae", "tenz", "tarik", "shahzam",
        "gaules", "ibai", "elrubius", "auronplay", "thegrefg",
        "rubius", "lolito_99", "djmaario", "alexby11", "bycalitos",
        "baitybait", "dakotaz", "kinggothalion", "goldglove", "drlupo",
        "chocotaco", "sypherpk", "couragejd", "nadeshot", "hiko",
        "skadoodle", "nothing", "stewie2k", "elige", "twitch",
        "riotgames", "blizzard", "esl_csgo", "faceittv", "dreamhackcs",
        "rocketleague", "minecraft", "fortnite", "valorant", "leagueoflegends",
        "counterstrike", "dota2ti", "overwatch", "apexlegends", "callofduty",
        "wirtual", "cdewx", "grandpoobear", "ryquek", "lilbowser1",
        "papaplatte", "trymacs", "handofblood", "bastighg", "knallerfrau",
        "gronkh", "ungespielt", "pietsmiet", "dhalucard", "robin_yo",
        "kikagaku", "kazu", "addi", "shisheyu", "shibainu",
        "katsudon", "remiroro", "sasakibros", "houshoumarine", "usadapekora",
        "shirakamifubuki", "ookamimio", "natsuiromatsuri", "akukin", "nakiriayame",
        "yozoramel", "hoshimachisuisei", "sakuratamochi", "tsunomakiwatame", "nekomataokazu",
        "inuishinobu", "takaneui", "kamiyakoto", "kageyamahina", "mizugantf",
    ];
}
