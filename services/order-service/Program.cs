using OrderService;
using OrderService.Infrastructure;

// order-service (C#) — PRODUCER.
// Генерирует заказы в топик "orders" (ключ = customerId) и профили клиентов в compacted-топик "customer-profiles".
// HTTP API позволяет менять rate/acks/idempotence/... на лету — этим пользуется control-center.

var builder = WebApplication.CreateBuilder(args);

var kafka = builder.Configuration.GetSection("Kafka").Get<KafkaOptions>() ?? new KafkaOptions();
builder.Services.AddSingleton(kafka);
builder.Services.AddSingleton<LabLog>();
builder.Services.AddSingleton<DeliveryVerifier>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<DeliveryVerifier>());
builder.Services.AddSingleton<OrderProducer>();
builder.Services.AddHostedService<OrderGenerator>();

var app = builder.Build();

app.MapGet("/health", () => "ok");
app.MapGet("/api/stats", (OrderProducer p) => p.GetStats());
app.MapGet("/api/config", (OrderProducer p) => p.Settings);
app.MapPut("/api/config", (OrderProducer p, SettingsPatch patch) => p.ApplySettings(patch));
app.MapPost("/api/orders", (OrderProducer p, ManualOrderRequest req) => p.SendManualAsync(req));
app.MapPost("/api/burst", (OrderProducer p, BurstRequest req) => p.Burst(req.Count));
app.MapDelete("/api/profiles/{customerId}", (OrderProducer p, string customerId) => p.DeleteProfileAsync(customerId));

app.Lifetime.ApplicationStarted.Register(() =>
    app.Services.GetRequiredService<LabLog>().Add("info", $"order-service запущен, bootstrap: {kafka.BootstrapServers}",
        $"order-service started, bootstrap: {kafka.BootstrapServers}"));

app.Run();
