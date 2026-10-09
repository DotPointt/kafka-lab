using OrderProcessor;
using OrderProcessor.Infrastructure;

// order-processor (C#) — CONSUMER GROUP "order-processing".
// Читает orders, «обрабатывает» заказ, пишет платёж в payments (consume → transform → produce).
// Неудачные заказы после ретраев уходят в orders.dlq. Количество консюмеров меняется на лету через HTTP API.

var builder = WebApplication.CreateBuilder(args);

var kafka = builder.Configuration.GetSection("Kafka").Get<KafkaOptions>() ?? new KafkaOptions();
builder.Services.AddSingleton(kafka);
builder.Services.AddSingleton<LabLog>();
builder.Services.AddSingleton<ProcessorContext>();
builder.Services.AddSingleton<ConsumerManager>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<ConsumerManager>());

var app = builder.Build();

app.MapGet("/health", () => "ok");
app.MapGet("/api/stats", (ConsumerManager m) => m.GetStats());
app.MapGet("/api/config", (ProcessorContext c) => c.Settings);
app.MapPut("/api/config", (ConsumerManager m, SettingsPatch patch) => m.ApplySettings(patch));
app.MapPost("/api/instances", (ConsumerManager m) => m.Add());
app.MapDelete("/api/instances/{id:int}", (ConsumerManager m, int id) => m.Remove(id));
app.MapPost("/api/instances/{id:int}/crash", (ConsumerManager m, int id) => m.Crash(id));
app.MapPost("/api/instances/{id:int}/restart", (ConsumerManager m, int id) => m.Restart(id));
app.MapPost("/api/instances/{id:int}/stuck", (ConsumerManager m, int id, bool? stuck) => m.Stuck(id, stuck));

// Трассировка по correlation id: что этот сервис сделал с бизнес-транзакцией (и сколько раз).
app.MapGet("/api/trace/{cid}", (ProcessorContext c, string cid) => c.Trace.Get(cid));
app.MapGet("/api/traces", (ProcessorContext c, string? ids) => c.Trace.GetMany(ids));
app.MapGet("/api/traces/recent", (ProcessorContext c) => c.Trace.RecentView());

app.Run();
