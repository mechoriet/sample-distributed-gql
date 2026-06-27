using TwitchGqlMockServer;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSignalR();
builder.Services.AddHostedService<TestOperationService>();

var app = builder.Build();

app.MapHub<ProxyHub>("/hub");

app.Run();
