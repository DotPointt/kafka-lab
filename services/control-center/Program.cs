using System.Text.Json;
using System.Text.RegularExpressions;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using ControlCenter;
using ControlCenter.Chaos;
using ControlCenter.Docker;
using ControlCenter.Monitoring;
using ControlCenter.Tools;
using Microsoft.AspNetCore.StaticFiles;
using Microsoft.Extensions.FileProviders;

// control-center (C#) — «пульт управления» стендом:
//   • мониторит кластер через Kafka AdminClient и отдаёт снимок в браузер (SSE);
//   • управляет контейнерами через Docker API (stop/kill/pause) и портит сеть брокеров через tc;
//   • проксирует настройки сервисов (rate, acks, число консюмеров...);
//   • раздаёт UI (wwwroot) и памятки (docs/*.md).

var builder = WebApplication.CreateBuilder(args);
var options = builder.Configuration.GetSection("Lab").Get<LabOptions>() ?? new LabOptions();

if (args.Contains(GroupProbe.Flag))
{
    // Дочерний процесс опроса consumer groups (см. Monitoring/GroupProbe.cs)
    await GroupProbe.RunChildAsync(options);
    return;
}

builder.Services.AddSingleton(options);
builder.Services.AddSingleton(new DockerApi(options.DockerNetwork));
builder.Services.AddSingleton<EventStore>();
builder.Services.AddSingleton<SnapshotHub>();
builder.Services.AddSingleton<KafkaInspector>();
builder.Services.AddSingleton<ChaosManager>();
builder.Services.AddSingleton<ServicePoller>();
builder.Services.AddSingleton<MessagePeeker>();
builder.Services.AddSingleton<QuorumMonitor>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<QuorumMonitor>());
builder.Services.AddSingleton<GroupProbeSupervisor>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<GroupProbeSupervisor>());
builder.Services.AddSingleton<ClusterMonitor>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<ClusterMonitor>());
builder.Services.AddHttpClient("services", c => c.Timeout = TimeSpan.FromSeconds(45));

var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles(new StaticFileOptions
{
    OnPrepareResponse = ctx => ctx.Context.Response.Headers.CacheControl = "no-cache",
});
if (Directory.Exists(options.DocsPath))
{
    var types = new FileExtensionContentTypeProvider();
    types.Mappings[".md"] = "text/markdown; charset=utf-8";
    app.UseStaticFiles(new StaticFileOptions
    {
        FileProvider = new PhysicalFileProvider(options.DocsPath),
        RequestPath = "/docs",
        ContentTypeProvider = types,
        OnPrepareResponse = ctx => ctx.Context.Response.Headers.CacheControl = "no-cache",
    });
}

var events = app.Services.GetRequiredService<EventStore>();

// ------------------------------------------------------------------ состояние

app.MapGet("/api/snapshot", (SnapshotHub hub) => Results.Content(hub.LatestJson ?? "{}", "application/json"));
app.MapGet("/api/events", (EventStore store, long? since) => since is null ? store.Recent(500) : store.Since(since.Value));

// Server-Sent Events: раз в секунду снимок + новые события.
app.MapGet("/api/stream", async (HttpContext ctx, SnapshotHub hub, EventStore store, CancellationToken ct) =>
{
    ctx.Response.Headers.ContentType = "text/event-stream";
    ctx.Response.Headers.CacheControl = "no-cache";
    ctx.Response.Headers["X-Accel-Buffering"] = "no";

    var initial = store.Recent(400);
    var lastEvent = initial.Count > 0 ? initial[^1].Id : 0;
    await ctx.Response.WriteAsync($"event: events\ndata: {JsonSerializer.Serialize(initial, SnapshotHub.Json)}\n\n", ct);
    await ctx.Response.Body.FlushAsync(ct);

    long version = -1;
    try
    {
        while (!ct.IsCancellationRequested)
        {
            var (json, v) = await hub.WaitNextAsync(version, ct);
            version = v;
            await ctx.Response.WriteAsync($"event: snapshot\ndata: {json}\n\n", ct);
            var fresh = store.Since(lastEvent);
            if (fresh.Count > 0)
            {
                lastEvent = fresh[^1].Id;
                await ctx.Response.WriteAsync($"event: events\ndata: {JsonSerializer.Serialize(fresh, SnapshotHub.Json)}\n\n", ct);
            }
            await ctx.Response.Body.FlushAsync(ct);
        }
    }
    catch (OperationCanceledException)
    {
        // браузер закрыл вкладку
    }
});

// ------------------------------------------------------------------ сбои: контейнеры

string[] containerActions = ["start", "stop", "kill", "pause", "unpause", "restart"];

async Task<IResult> ContainerActionAsync(string name, string action, ClusterMonitor monitor, DockerApi docker, CancellationToken ct)
{
    if (!containerActions.Contains(action)) return Results.BadRequest(new { ok = false, error = $"Неизвестное действие {action}" });
    var containers = await monitor.RefreshContainersAsync(ct);
    var c = containers.FirstOrDefault(x => x.Name == name);
    if (c is null) return Results.NotFound(new { ok = false, error = $"Контейнер {name} не найден" });
    if (c.Service is "control-center" or "kafka-init") return Results.BadRequest(new { ok = false, error = "Этим контейнером управлять нельзя" });

    var isBroker = options.Brokers.Any(b => b.Container == name);
    var isConsumer = options.Services.Any(s => s.Name == c.Service && s.Role == "consumer");
    var (text, learn, level) = action switch
    {
        "stop" when isBroker => ($"⏹ docker stop {name}: SIGTERM → controlled shutdown — брокер сам передаёт лидерство другим репликам и выходит", "graceful-shutdown", "warn"),
        "stop" when isConsumer => ($"⏹ docker stop {name}: SIGTERM → консюмер коммитит offset-ы и отправляет LeaveGroup → немедленный ребаланс", "rebalance", "warn"),
        "stop" => ($"⏹ docker stop {name}: SIGTERM → корректное завершение", null, "warn"),
        "kill" when isBroker => ($"💥 docker kill {name}: SIGKILL — брокер умер мгновенно, без controlled shutdown. Контроллер узнает об этом по пропавшим heartbeat", "broker-down", "error"),
        "kill" when isConsumer => ($"💥 docker kill {name}: SIGKILL — консюмер умер без коммита и LeaveGroup. Группа ждёт session.timeout.ms", "consumer-crash", "error"),
        "kill" => ($"💥 docker kill {name}: SIGKILL — процесс умер мгновенно", null, "error"),
        "pause" => ($"⏸ docker pause {name}: процесс заморожен (как бесконечная GC-пауза)", "broker-pause", "warn"),
        "unpause" => ($"▶ docker unpause {name}", "broker-pause", "info"),
        "start" => ($"▶ docker start {name}", null, "info"),
        _ => ($"🔄 docker restart {name}", null, "info"),
    };
    events.Add(level, "action", text, learn);
    try
    {
        await docker.ActionAsync(c.Id, action, ct);
        return Results.Ok(new { ok = true });
    }
    catch (Exception ex)
    {
        events.Add("error", "action", $"{name}: {action} не удался — {ex.Message}");
        return Results.Json(new { ok = false, error = ex.Message }, statusCode: 409);
    }
}

app.MapPost("/api/containers/{name}/{action}", (string name, string action, ClusterMonitor m, DockerApi d, CancellationToken ct) =>
    ContainerActionAsync(name, action, m, d, ct));

app.MapPost("/api/brokers/{id:int}/network", async (int id, NetworkChaos body, ClusterMonitor monitor, ChaosManager chaos, CancellationToken ct) =>
{
    var latency = Math.Clamp(body.LatencyMs, 0, 10000);
    var chaosSpec = new NetworkChaos(latency, Math.Clamp(body.JitterMs, 0, latency), Math.Clamp(body.LossPct, 0, 100), body.Isolated, body.SplitFromBrokers);
    var containers = await monitor.RefreshContainersAsync(ct);
    var (ok, message) = await chaos.SetNetworkAsync(id, chaosSpec, containers, ct);
    if (!ok) events.Add("error", "action", $"Сеть брокера {id}: {message}");
    return ok ? Results.Ok(new { ok, output = message }) : Results.Json(new { ok, error = message }, statusCode: 409);
});

app.MapPost("/api/brokers/{id:int}/{action}", (int id, string action, ClusterMonitor m, DockerApi d, CancellationToken ct) =>
{
    var def = options.Brokers.FirstOrDefault(b => b.Id == id);
    return def is null ? Task.FromResult(Results.NotFound()) : ContainerActionAsync(def.Container, action, m, d, ct);
});

app.MapPost("/api/heal-all", async (ClusterMonitor monitor, DockerApi docker, ChaosManager chaos, CancellationToken ct) =>
{
    events.Add("info", "action", "🩹 «Починить всё»: снять паузы, вернуть сеть, запустить остановленные контейнеры");
    var log = new List<string>();
    var containers = await monitor.RefreshContainersAsync(ct);
    var managed = containers.Where(c => c.Service is not ("control-center" or "kafka-init")).ToList();
    foreach (var c in managed.Where(c => c.State == "paused"))
    {
        try { await docker.ActionAsync(c.Id, "unpause", ct); log.Add($"unpause {c.Name}"); } catch (Exception ex) { log.Add($"{c.Name}: {ex.Message}"); }
    }
    containers = await monitor.RefreshContainersAsync(ct);
    foreach (var b in options.Brokers.Where(b => !chaos.Get(b.Id).IsNone))
    {
        var (ok, msg) = await chaos.SetNetworkAsync(b.Id, new NetworkChaos(), containers, ct);
        log.Add(ok ? $"сеть kafka-{b.Id} восстановлена" : msg);
    }
    foreach (var c in managed.Where(c => c.State is "exited" or "created" or "dead").OrderBy(c => options.Brokers.Any(b => b.Container == c.Name) ? 0 : 1))
    {
        try { await docker.ActionAsync(c.Id, "start", ct); log.Add($"start {c.Name}"); } catch (Exception ex) { log.Add($"{c.Name}: {ex.Message}"); }
    }
    return Results.Ok(new { ok = true, log });
});

// ------------------------------------------------------------------ админ-операции Kafka

app.MapPost("/api/leaders/preferred", async (KafkaInspector kafka, SnapshotHub hub) =>
{
    var parts = hub.Latest?.Topics.Where(t => !t.Internal)
        .SelectMany(t => t.Partitions.Select(p => new TopicPartition(t.Name, p.Id))).ToList() ?? [];
    events.Add("info", "action", "⚖ Выборы предпочтительных лидеров (аналог kafka-leader-election.sh --election-type PREFERRED --all-topic-partitions)", "preferred-leader");
    List<TopicPartitionError> results;
    try
    {
        results = (await kafka.Admin.ElectLeadersAsync(ElectionType.Preferred, parts,
            new ElectLeadersOptions { RequestTimeout = TimeSpan.FromSeconds(15), OperationTimeout = TimeSpan.FromSeconds(15) })).TopicPartitions;
    }
    catch (ElectLeadersException ex)
    {
        results = ex.Results.TopicPartitions;
    }
    catch (Exception ex)
    {
        return Results.Json(new { ok = false, error = ex.Message }, statusCode: 500);
    }
    var elected = results.Count(r => !r.Error.IsError);
    var notNeeded = results.Count(r => r.Error.Code == ErrorCode.ElectionNotNeeded);
    var failed = results.Where(r => r.Error.IsError && r.Error.Code != ErrorCode.ElectionNotNeeded)
        .Select(r => $"{r.Topic}-{r.Partition.Value}: {r.Error.Reason}").ToList();
    events.Add(failed.Count > 0 ? "warn" : "success", "action",
        $"Предпочтительные лидеры: переизбрано {elected}, уже на месте {notNeeded}" + (failed.Count > 0 ? $", не удалось {failed.Count}" : ""), "preferred-leader");
    return Results.Ok(new { ok = true, elected, notNeeded, failed });
});

app.MapPost("/api/topics/{topic}/partitions", async (string topic, PartitionsRequest req, KafkaInspector kafka) =>
{
    try
    {
        await kafka.Admin.CreatePartitionsAsync([new PartitionsSpecification { Topic = topic, IncreaseTo = req.Count }]);
        kafka.InvalidateConfigs();
        events.Add("warn", "action", $"➕ Топик «{topic}» расширен до {req.Count} партиций (kafka-topics.sh --alter --partitions {req.Count}). Уменьшить число партиций нельзя!", "partitions-increase");
        return Results.Ok(new { ok = true });
    }
    catch (CreatePartitionsException ex)
    {
        var reason = ex.Results.FirstOrDefault()?.Error.Reason ?? ex.Message;
        return Results.Json(new { ok = false, error = reason }, statusCode: 409);
    }
});

app.MapPost("/api/topics/{topic}/config", async (string topic, TopicConfigRequest req, KafkaInspector kafka) =>
{
    string[] allowed = ["min.insync.replicas", "retention.ms", "retention.bytes", "segment.ms", "min.cleanable.dirty.ratio", "unclean.leader.election.enable"];
    if (!allowed.Contains(req.Key)) return Results.BadRequest(new { ok = false, error = $"Разрешено менять: {string.Join(", ", allowed)}" });
    try
    {
        var resource = new ConfigResource { Type = ResourceType.Topic, Name = topic };
        await kafka.Admin.IncrementalAlterConfigsAsync(new Dictionary<ConfigResource, List<ConfigEntry>>
        {
            [resource] = [new ConfigEntry { Name = req.Key, Value = req.Value, IncrementalOperation = AlterConfigOpType.Set }],
        });
        kafka.InvalidateConfigs();
        events.Add("warn", "action", $"⚙ {topic}: {req.Key}={req.Value} (kafka-configs.sh --alter --entity-type topics --entity-name {topic} --add-config {req.Key}={req.Value})",
            req.Key == "min.insync.replicas" ? "min-isr" : null);
        return Results.Ok(new { ok = true });
    }
    catch (Exception ex)
    {
        return Results.Json(new { ok = false, error = ex.Message }, statusCode: 409);
    }
});

app.MapPost("/api/groups/{group}/reset", async (string group, ResetRequest req, ClusterMonitor monitor, DockerApi docker, ChaosManager chaos, CancellationToken ct) =>
{
    if (!Regex.IsMatch(group, "^[A-Za-z0-9._-]+$")) return Results.BadRequest(new { ok = false, error = "Некорректное имя группы" });
    string[] target = req.To switch
    {
        "earliest" => ["--to-earliest"],
        "latest" => ["--to-latest"],
        "shift" => ["--shift-by", Math.Clamp(req.ShiftBy ?? -100, -1_000_000, 1_000_000).ToString()],
        _ => [],
    };
    if (target.Length == 0) return Results.BadRequest(new { ok = false, error = "to: earliest | latest | shift" });

    var containers = await monitor.RefreshContainersAsync(ct);
    var broker = options.Brokers
        .Select(b => (Def: b, C: containers.FirstOrDefault(c => c.Name == b.Container)))
        .FirstOrDefault(x => x.C?.State == "running" && chaos.Get(x.Def.Id).IsNone);
    if (broker.C is null) return Results.Json(new { ok = false, error = "Нет здорового брокера для запуска CLI" }, statusCode: 409);

    string[] args = ["--bootstrap-server", "localhost:9092", "--group", group, "--reset-offsets", .. target, "--all-topics", "--execute"];
    var shown = "kafka-consumer-groups.sh " + string.Join(' ', args);
    events.Add("info", "action", $"⏪ {shown}", "replay");
    var result = await docker.ExecAsync(broker.C.Id, ["/opt/kafka/bin/kafka-consumer-groups.sh", .. args], "appuser",
        ["KAFKA_HEAP_OPTS=-Xmx128m", "KAFKA_JVM_PERFORMANCE_OPTS=-XX:+UseSerialGC -XX:TieredStopAtLevel=1"], ct);
    var output = (result.StdOut + "\n" + result.StdErr).Trim();
    var ok = result.ExitCode == 0 && !output.Contains("Error", StringComparison.OrdinalIgnoreCase);
    var firstLine = output.Split('\n').FirstOrDefault(l => l.Contains("Error", StringComparison.OrdinalIgnoreCase)) ?? "";
    events.Add(ok ? "success" : "error", "action",
        ok ? $"Offset-ы группы «{group}» сброшены ({req.To}). При следующем подключении консюмеры начнут читать с новой позиции"
           : $"Сброс offset-ов не выполнен: {firstLine.Trim()} — сначала останови все консюмеры группы", "replay");
    return Results.Ok(new { ok, command = shown, output });
});

app.MapPost("/api/produce", async (ProduceRequest req, MessagePeeker peeker) =>
{
    var result = await peeker.ProduceAsync(req.Topic, req.Key, req.Value, req.Headers);
    events.Add("info", "action", $"✉ Отправлено вручную в «{req.Topic}» (key={req.Key ?? "null"}): {Shorten(req.Value)}", req.Note);
    return Results.Ok(result);
});

app.MapGet("/api/topics/{topic}/messages", (string topic, int? partition, int? limit, MessagePeeker peeker, SnapshotHub hub, CancellationToken ct) =>
{
    IReadOnlyList<int> parts = partition is not null
        ? [partition.Value]
        : hub.Latest?.Topics.FirstOrDefault(t => t.Name == topic)?.Partitions.Select(p => p.Id).ToList() ?? [0];
    return peeker.PeekAsync(topic, parts, limit ?? 30, ct);
});

// ------------------------------------------------------------------ сервисы (прокси) и документы

app.Map("/api/svc/{service}/{**path}", (string service, string path, HttpRequest request, ServicePoller poller, ClusterMonitor monitor, CancellationToken ct) =>
    poller.ProxyAsync(service, path, request, monitor.Containers, ct));

app.MapGet("/api/docs", () =>
{
    if (!Directory.Exists(options.DocsPath)) return Results.Ok(Array.Empty<object>());
    var docs = Directory.GetFiles(options.DocsPath, "*.md").OrderBy(f => f).Select(f =>
    {
        var title = File.ReadLines(f).FirstOrDefault(l => l.StartsWith("# "))?[2..].Trim() ?? Path.GetFileNameWithoutExtension(f);
        return new { file = Path.GetFileName(f), title };
    });
    return Results.Ok(docs);
});

app.Run();

static string Shorten(string? s) => s is null ? "null (tombstone)" : s.Length > 80 ? s[..80] + "…" : s;

public sealed record PartitionsRequest(int Count);
public sealed record TopicConfigRequest(string Key, string Value);
public sealed record ResetRequest(string To, long? ShiftBy);
public sealed record ProduceRequest(string Topic, string? Key, string? Value, Dictionary<string, string>? Headers, string? Note);
