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
  │  │             │     │   uc <ch>   ──► UserCards     │
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
       │     "backend.UserCards"        │
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
    .UserCards") ───────►│                            │
                         │                            │
       │           Enqueue to batch queue              │
       │           PendingCount ++                     │
       │                 │                            │
       │           (queue fills to MaxBatchSize=20)    │
       │                 │                            │
  SendAsync(             │                            │
   "backend              │                            │
    .UserCards") ───────►│                            │
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
    .UserCards")         │                            │
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
- **`backend.UserCards`** — queued, batched up to `MaxBatchSize`, then flushed

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

## Configuration (TwitchGqlProxy)

| Setting | Env Var | Default | Description |
|---|---|---|---|
| `signalrHubUrl` | `SIGNALR_HUB_URL` | `http://localhost:5000/hub` | MockServer SignalR endpoint |
| `gqlEndpoint` | `GQL_ENDPOINT` | `https://gql.twitch.tv/gql` | Twitch GQL API |
| `clientId` | `CLIENT_ID` | **(required)** | Twitch Client-Id header |
| `channels` | `CHANNELS` | `backend.communityTab,backend.UserCards` | SignalR channels to listen on |
| `minBatchSize` | `MIN_BATCH_SIZE` | `5` | Min batch before flush (debounce trigger) |
| `maxBatchSize` | `MAX_BATCH_SIZE` | `20` | Max operations per GQL request |
| `debounceMs` | `DEBOUNCE_MS` | `300` | Debounce timer ms |
| `rateLimitPerMinute` | `RATE_LIMIT` | `5000` | Max requests per minute to GQL |

Settings are read from `appsettings.json` (in the output directory) with environment variables overriding. `CLIENT_ID` must always be set via env var.

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
| `uc <channel>` | `uc xqc` | Sends UserCards op to a random proxy |
| `batch <n> [ch]` | `batch 50 forsen` | Sends n ViewerCard operations |
| `q` | `q` | Quit |

### Proxy

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
