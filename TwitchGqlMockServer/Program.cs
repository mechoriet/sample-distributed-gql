using Microsoft.AspNetCore.Authentication;
using TwitchGqlMockServer;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddAuthentication("Token")
    .AddScheme<AuthenticationSchemeOptions, TokenAuthenticationHandler>("Token", null);
builder.Services.AddAuthorization();

builder.Services.AddSignalR();
builder.Services.AddHostedService<TestOperationService>();

var app = builder.Build();

app.UseAuthentication();
app.UseAuthorization();

app.MapHub<ProxyHub>("/hub").RequireAuthorization();

app.Run();
