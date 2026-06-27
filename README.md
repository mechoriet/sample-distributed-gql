# Twitch GQL Remote

A pair of .NET applications that relay and batch Twitch GraphQL operations via SignalR.

```
  TwitchGqlMockServer          TwitchGqlProxy          Twitch GQL
  (test harness)               (production proxy)      (live API)
  ┌─────────────────┐          ┌─────────────────┐     ┌──────────┐
  │  SignalR Hub     │◄──SignalR──► SignalR Client  │     │          │
  │  /hub            │          │                 │────►│  gql.     │
  │                  │          │  HTTP Client    │     │  twitch   │
  │  TestOperation   │          │  (batched POST)  │     │  .tv/gql  │
  │  Service         │          │                 │     │          │
  └─────────────────┘          └─────────────────┘     └──────────┘
```

---

## Running

Start the MockServer first, then the Proxy.

```
# Terminal 1 — MockServer
cd TwitchGqlMockServer
dotnet run

# Terminal 2 — Proxy
cd TwitchGqlProxy
dotnet run
```

The Proxy connects to the MockServer's SignalR hub at `http://localhost:5000/hub`. Once connected, type test commands into the MockServer terminal to trigger GQL operations through the Proxy.

---

## Architecture

### TwitchGqlMockServer

A SignalR hub that simulates Twitch GQL responses. It accepts proxy client connections and lets you trigger test operations interactively.

```
  ┌──────────────────────────────────────────────────────┐
  │               TwitchGqlMockServer                     │
  │                                                       │
  │  ┌─────────────┐     ┌──────────────────────────┐    │
  │  │  ProxyHub   │     │  TestOperationService    │    │
  │  │  (SignalR)  │     │  (BackgroundService)     │    │
  │  │             │     │                          │    │
  │  │ OnConnected │────►│  Reads stdin commands:   │    │
  │  │ OnMessage   │     │   ct <ch>   ──► CommunityTab │
  │  │             │     │   uc <ch>   ──► ViewerCards     │
  │  │ Track       │     │   batch N   ──► Batch Viewer  │
  │  │ clients in  │     │                Card ops       │
  │  │ Concurrent  │     │                          │    │
  │  │ Dictionary  │     │  Picks random connected  │    │
  │  │             │     │  client and sends via    │    │
  │  │             │     │  IHubContext             │    │
  │  └─────────────┘     └──────────────────────────┘    │
  └──────────────────────────────────────────────────────┘
```

### TwitchGqlProxy

A console app that connects to the MockServer's SignalR hub, listens for GQL operation notifications, and forwards them in batches to the real Twitch GQL API.

```
  ┌──────────────────────────────────────────────────────────┐
  │                   TwitchGqlProxy                          │
  │                                                           │
  │  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐  │
  │  │  SignalR     │   │  Batcher     │   │  HTTP Client │  │
  │  │  Connection  │──►│              │──►│              │  │
  │  │              │   │  Concurrency │   │  POST to GQL │  │
  │  │  On("backend │   │  -Queue     │   │  Endpoint    │  │
  │  │    .XXX")    │   │  -Pending    │   │              │  │
  │  │              │   │  Count       │   │  Rate        │  │
  │  │  Receives    │   │  -FlushLock  │   │  Limiter     │  │
  │  │  JSON ops    │   │              │   │  (sliding    │  │
  │  │              │   │  CommunityTab│   │   window)    │  │
  │  │  Sends       │   │    → flush   │   │              │  │
  │  │  responses   │   │    immediate │   │  Client-Id   │  │
  │  │  back via    │   │              │   │  header      │  │
  │  │  InvokeAsync │   │  ViewerCard  │   │              │  │
  │  │              │   │    → queue,  │   └──────────────┘  │
  │  │              │   │      batch   │                     │
  │  │              │   │      at 20   │                     │
  │  └──────────────┘   └──────────────┘                     │
  └──────────────────────────────────────────────────────────┘
```

---

## SignalR Message Flow

### Connection Setup

```
  TwitchGqlProxy                  TwitchGqlMockServer
       │                                │
       │  ─── HTTP / SignalR ──────────►│
       │     negotiate + connect        │
       │                                │
       │  ◄── Connection Established ── │
       │                                │
       │  ─── Subscribe to channels ──► │
       │     "backend.communityTab"     │
       │     "backend.ViewerCards"        │
       │                                │
       │  Hub registers OnConnected     │
       │  adds client to                │
       │  ConnectedClients dictionary   │
```

### Operation Flow (CommunityTab — pass-through)

```
  MockServer            Proxy                      Twitch GQL
  TestOpService          │                            │
       │                 │                            │
  SendAsync(             │                            │
   "backend              │                            │
    .communityTab") ────►│                            │
                         │                            │
       │           Parse & wrap as GQL request        │
       │           POST ────────────────────────────► │
       │                 │                            │
       │           ◄──── 200 + response ───────────── │
       │                 │                            │
       │           Send{status:200,channel,b.body}    │
  InvokeAsync( ◄────────│                            │
   "backend              │                            │
    .communityTab")      │                            │
```

### Operation Flow (ViewerCard — batched)

```
  MockServer            Proxy                      Twitch GQL
  TestOpService          │                            │
       │                 │                            │
  SendAsync(             │                            │
   "backend              │                            │
    .ViewerCards") ───────►│                            │
                         │                            │
       │           Enqueue to batch queue              │
       │           PendingCount ++                     │
       │                 │                            │
       │           (queue fills to MaxBatchSize=20)    │
       │                 │                            │
  SendAsync(             │                            │
   "backend              │                            │
    .ViewerCards") ───────►│                            │
       │                 │                            │
       │           PendingCount == 20                  │
       │           DrainQueue → List<BatchItem>        │
       │                 │                            │
       │           POST [item, item, ...] ──────────► │
       │                 │                            │
       │           ◄──── [{...},{...},...] ────────── │
       │                 │                            │
       │           Fan out responses back              │
  InvokeAsync( ◄────────│  via InvokeAsync             │
   "backend              │  per item                    │
    .ViewerCards")         │                            │
```

---

## Batching Logic Detail

```
  ViewerCard arrives
        │
        ▼
  ┌────────────────┐
  │  Lock(_flush)  │
  │  Queue.Enqueue │
  │  PendingCount++│
  └───────┬────────┘
          │
          ▼
  PendingCount >= MaxBatchSize?
        │            │
       YES           NO
        │            │
        ▼            ▼
  DrainQueue()    Wait for more
  FlushBatch()    items
        │
        ▼
  ┌─────────────────────────────────┐
  │  Serialize batch as JSON array  │
  │  [elem0, elem1, ..., elemN]     │
  ├─────────────────────────────────┤
  │  EnforceRateLimit()             │
  │  (sliding window, per-minute    │
  │   cap)                          │
  ├─────────────────────────────────┤
  │  POST to GQL endpoint           │
  ├─────────────────────────────────┤
  │  Parse response array           │
  ├─────────────────────────────────┤
  │  For each response:             │
  │    Build {status, channel, body}│
  │    SendResponse → InvokeAsync   │
  │    back to MockServer           │
  └─────────────────────────────────┘
```

### Why Two SignalR Channels?

- **`backend.communityTab`** — flushed immediately (single operation per request)
- **`backend.ViewerCards`** — queued, batched up to `MaxBatchSize`, then flushed

The `responseChannel` in each `BatchItem` stores which SignalR method to use when sending the response back. This decouples the incoming notification channel from the outgoing response channel.

---

## Rate Limiting

A sliding-window rate limiter prevents hitting Twitch's API limits:

```
  Window: 60 seconds
  Max:    configurable (default 5000 req/min)

  ──┬─────┬─────┬─────┬─────┬─────┬─────┬──► time
    │  t1 │  t2 │  t3 │     │     │  now│
    └─────┴─────┴─────┘     └─────────────┘
    │ < 60s, within limit       │
    ▼                          ▼
  Window slides,           New request
  old entries              allowed
  evicted
```

If the limit is hit, the proxy delays until a slot opens.

---

## Authentication

The MockServer supports optional bearer-token authentication for the SignalR hub.

```
  Proxy                          MockServer
    │                                │
    │  GET /hub (negotiate)          │
    │  Authorization: Bearer <token> │
    │──────────────────────────────► │
    │                                │
    │  ◄── 401 (if token wrong/missing and auth is configured)
    │                                │
    │  ◄── 200 + connection (if token valid or auth disabled)
```

### How it works

**Server** — A custom `TokenAuthenticationHandler` (ASP.NET Core `AuthenticationHandler`) checks the `Authorization: Bearer <token>` header against the `authToken` configured in `appsettings.json`. If `authToken` is empty (default), authentication is bypassed and all clients are allowed.

**Client** — The Proxy sends the token via SignalR's built-in `AccessTokenProvider`, which sets the `Authorization: Bearer` header on the negotiate request.

### Configuration

| Setting | Default | Description |
|---|---|---|
| `authToken` | `""` (disabled) | Shared secret. Set the same value on both server and client to enforce auth. |

Set `"authToken": "my-secret"` in both `TwitchGqlMockServer/appsettings.json` and `TwitchGqlProxy/appsettings.json`. Any client without the matching token gets a 401 on connect.

### Per-hub authentication

Authentication is applied per-hub via `.RequireAuthorization()` on the endpoint mapping. Hubs mapped without it are public.

```csharp
// Program.cs

// Secured — clients need the bearer token
app.MapHub<ProxyHub>("/hub").RequireAuthorization();

// Public — no auth required
app.MapHub<PublicHub>("/public");   // <-- any client can connect

// Also public
app.MapHub<AnotherHub>("/another").AllowAnonymous();
```

This makes it easy to add mixed-access endpoints: keep your production proxy hub locked down while exposing a public status/debug hub on the same server.

---

## Configuration (TwitchGqlProxy)

| Setting | Env Var | Default | Description |
|---|---|---|---|
| `signalrHubUrl` | `SIGNALR_HUB_URL` | `http://localhost:5000/hub` | MockServer SignalR endpoint |
| `gqlEndpoint` | `GQL_ENDPOINT` | `https://gql.twitch.tv/gql` | Twitch GQL API |
| `clientId` | `CLIENT_ID` | **(required)** | Twitch Client-Id header. Set in `appsettings.json` or via env var. |
| `minBatchSize` | `MIN_BATCH_SIZE` | `5` | Min batch before flush (debounce trigger) |
| `maxBatchSize` | `MAX_BATCH_SIZE` | `20` | Max operations per GQL request |
| `debounceMs` | `DEBOUNCE_MS` | `300` | Debounce timer ms |
| `rateLimitPerMinute` | `RATE_LIMIT` | `5000` | Max requests per minute to GQL |
| `authToken` | — | `""` (disabled) | Bearer token for SignalR hub auth |

SignalR channels (`backend.communityTab`, `backend.ViewerCards`) are hardcoded in `ProxyService` — they are not configurable.

Settings are read from `appsettings.json` (in the output directory) with environment variables overriding. `clientId` is required — set it in `appsettings.json` or pass it as the `CLIENT_ID` environment variable.

---

## Usage

### MockServer

```
dotnet run --project TwitchGqlMockServer
```

Then type commands at the prompt:

| Command | Example | Action |
|---|---|---|
| `ct <channel>` | `ct forsen` | Sends CommunityTab op to a random proxy |
| `uc <channel>` | `uc xqc` | Sends ViewerCards op to a random proxy |
| `batch <n> [ch]` | `batch 50 forsen` | Sends n ViewerCard operations |
| `q` | `q` | Quit |

### Proxy

Set `clientId` in `appsettings.json`, then:

```
dotnet run --project TwitchGqlProxy
```

Or via env var override:

```
CLIENT_ID=your_twitch_client_id dotnet run --project TwitchGqlProxy
```

---

## Project Structure

```
twitchscanapi-gql-remote/
├── .gitignore
├── README.md
├── TwitchGqlMockServer/
│   ├── Program.cs                  # ASP.NET entry, maps /hub
│   ├── ProxyHub.cs                 # SignalR hub, tracks clients, logs responses
│   ├── TestOperationService.cs     # Background service, stdin commands
│   ├── TokenAuthenticationHandler.cs  # Bearer token auth for SignalR hub
│   ├── TwitchGqlMockServer.csproj
│   ├── appsettings.json
│   └── Properties/
│       └── launchSettings.json
└── TwitchGqlProxy/
    ├── Program.cs                  # Console entry, DI setup, Ctrl+C handling
    ├── ProxyService.cs             # Core: SignalR client, batching, GQL POST
    ├── TwitchGqlProxy.csproj
    └── appsettings.json            # All non-secret settings
```
