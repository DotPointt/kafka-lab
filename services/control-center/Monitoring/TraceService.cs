using System.Text.Json;

namespace ControlCenter.Monitoring;

/// <summary>Шаг бизнес-транзакции, как его записал сервис (см. TraceLog в сервисах).</summary>
public sealed record TraceStepView(long Ts, string Service, string Instance, string Step, string State,
    string? Topic, int? Partition, long? Offset, string? Detail);

/// <summary>
/// Этап пути события. Status: ok (ровно один раз), duplicate (больше одного раза), lost, failed,
/// pending (ещё ждём), missing (ждём дольше 30 c — застряло или потерялось), skipped (не нужен, например после DLQ),
/// unverified (аудитор лога не видел запись, но консюмеры её прочитали).
/// </summary>
public sealed record TraceStage(string Id, string Service, string? Topic, int Count, string Status, string? State,
    long? Ts, List<string> Instances);

public sealed record TraceView(string Cid, bool Found, string Verdict, string BusinessState, long? FirstTs, long? LastTs, long Now,
    List<TraceStage> Stages, List<TraceStepView> Steps, List<string> Unreachable);

/// <summary>
/// Трассировка одной бизнес-транзакции по correlation id. Каждый сервис помнит, что ОН сделал с событием
/// (GET /api/trace/{cid}); control-center опрашивает все экземпляры, склеивает шаги по времени
/// и проверяет главное: бизнес-состояние сменилось в каждом сервисе ровно один раз — не потерялось и не задвоилось.
/// </summary>
public sealed class TraceService(IHttpClientFactory httpFactory, LabOptions options, ClusterMonitor monitor)
{
    private static readonly string[] Traced = ["order-service", "order-processor", "analytics"];
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private const long StuckMs = 30_000;

    public async Task<TraceView> TraceAsync(string cid, CancellationToken ct)
    {
        var (bodies, unreachable) = await FanOutAsync($"trace/{Uri.EscapeDataString(cid)}", ct);
        var steps = bodies.SelectMany(b => Parse<List<TraceStepView>>(b) ?? []).ToList();
        return Analyze(cid, steps, unreachable);
    }

    /// <summary>Последние заказы + «проблемные» транзакции (дубли, потери, DLQ) со свежими вердиктами.</summary>
    public async Task<object> RecentAsync(CancellationToken ct)
    {
        var (bodies, unreachable) = await FanOutAsync("traces/recent", ct);
        var recent = new List<(long Ts, string Cid, string OrderId, string Customer, bool Manual)>();
        var anomalies = new List<(long Ts, string Cid, string Kind, string Instance, string? Detail)>();
        foreach (var b in bodies)
        {
            if (b.TryGetProperty("recent", out var r))
                foreach (var x in r.EnumerateArray())
                    recent.Add((x.GetProperty("ts").GetInt64(), Str(x, "cid") ?? "", Str(x, "orderId") ?? "", Str(x, "customer") ?? "",
                        x.TryGetProperty("manual", out var m) && m.ValueKind == JsonValueKind.True));
            if (b.TryGetProperty("anomalies", out var a))
                foreach (var x in a.EnumerateArray())
                    anomalies.Add((x.GetProperty("ts").GetInt64(), Str(x, "cid") ?? "", Str(x, "kind") ?? "", Str(x, "instance") ?? "", Str(x, "detail")));
        }

        var manual = recent.Where(r => r.Manual).OrderByDescending(r => r.Ts).Take(8).ToList();
        var latest = recent.Where(r => !r.Manual).OrderByDescending(r => r.Ts).Take(6).ToList();
        var problems = anomalies.Where(a => a.Cid != "").GroupBy(a => a.Cid)
            .Select(g => (Cid: g.Key, Ts: g.Max(x => x.Ts), Kinds: g.Select(x => x.Kind).Distinct().ToList(),
                Instances: g.Select(x => x.Instance).Distinct().ToList(), Details: g.Select(x => x.Detail).Where(d => d is not null).Distinct().Take(3).ToList()))
            .OrderByDescending(p => p.Ts).Take(25).ToList();

        var ids = manual.Select(r => r.Cid).Concat(latest.Select(r => r.Cid)).Concat(problems.Select(p => p.Cid)).Distinct().ToList();
        var byId = new Dictionary<string, List<TraceStepView>>();
        if (ids.Count > 0)
        {
            var (batch, _) = await FanOutAsync("traces?ids=" + Uri.EscapeDataString(string.Join(',', ids)), ct);
            foreach (var b in batch)
                if (b.ValueKind == JsonValueKind.Object)
                    foreach (var p in b.EnumerateObject())
                    {
                        if (!byId.TryGetValue(p.Name, out var list)) byId[p.Name] = list = [];
                        list.AddRange(Parse<List<TraceStepView>>(p.Value) ?? []);
                    }
        }
        TraceView Verdict(string cid) => Analyze(cid, byId.GetValueOrDefault(cid) ?? [], unreachable);

        return new
        {
            now = Now,
            recent = manual.Concat(latest).Select(r =>
            {
                var v = Verdict(r.Cid);
                return new { cid = r.Cid, orderId = r.OrderId, customer = r.Customer, ts = r.Ts, manual = r.Manual, verdict = v.Verdict, businessState = v.BusinessState };
            }),
            problems = problems.Select(p =>
            {
                var v = Verdict(p.Cid);
                return new { cid = p.Cid, ts = p.Ts, kinds = p.Kinds, instances = p.Instances, details = p.Details, verdict = v.Verdict, businessState = v.BusinessState };
            }),
            unreachable,
        };
    }

    // ------------------------------------------------------------------ анализ пути события

    public static TraceView Analyze(string cid, List<TraceStepView> all, List<string> unreachable)
    {
        var now = Now;
        var steps = all.OrderBy(s => s.Ts).ToList();
        int C(string step) => steps.Count(s => s.Step == step);
        long? LastTs(params string[] names) => steps.LastOrDefault(s => names.Contains(s.Step))?.Ts;

        var created = C("created"); var published = C("published"); var publishFailed = C("publish-failed");
        var inLog = C("in-log"); var lost = C("lost");
        var received = C("received"); var dlq = C("dead-lettered");
        var paid = C("paid"); var payFailed = C("payment-failed");
        var countedOrder = C("counted-order"); var countedPayment = C("counted-payment");

        // pending — следующий этап ещё не наступил; missing — его нет дольше 30 c после предыдущего
        string Wait(long? since) => since is null ? "pending" : now - since > StuckMs ? "missing" : "pending";

        var produce = published > 1 ? "duplicate" : published == 1 ? "ok" : publishFailed > 0 ? "failed"
            : created > 0 ? Wait(LastTs("created")) : "pending";
        var log = lost > 0 ? "lost" : inLog > 1 ? "duplicate" : inLog == 1 ? "ok" : received > 0 ? "unverified"
            : published > 0 ? Wait(LastTs("published")) : publishFailed > 0 ? "skipped" : "pending";
        var process = received > 1 ? "duplicate" : dlq > 0 ? "failed" : received == 1 ? "ok" : lost > 0 ? "lost"
            : publishFailed > 0 && published == 0 ? "skipped" : published > 0 ? Wait(LastTs("published", "in-log")) : "pending";
        var payment = paid > 1 ? "duplicate" : paid == 1 ? "ok" : dlq > 0 ? "skipped" : payFailed > 0 ? "failed" : lost > 0 ? "lost"
            : publishFailed > 0 && published == 0 ? "skipped" : received > 0 ? Wait(LastTs("received", "retry")) : "pending";
        var analyticsOrder = countedOrder > 1 ? "duplicate" : countedOrder == 1 ? "ok" : lost > 0 ? "lost"
            : publishFailed > 0 && published == 0 ? "skipped" : published > 0 ? Wait(LastTs("published", "in-log")) : "pending";
        var analyticsPayment = countedPayment > 1 ? "duplicate" : countedPayment == 1 ? "ok"
            : dlq > 0 || (paid == 0 && payFailed > 0) || lost > 0 || (publishFailed > 0 && published == 0) ? "skipped"
            : paid > 0 ? Wait(LastTs("paid")) : "pending";

        TraceStage Stage(string id, string service, string? topic, int count, string status, params string[] names)
        {
            var own = steps.Where(s => names.Contains(s.Step)).ToList();
            return new TraceStage(id, service, topic, count, status, own.LastOrDefault()?.State, own.LastOrDefault()?.Ts,
                own.Select(s => s.Instance).Distinct().ToList());
        }

        var stages = new List<TraceStage>
        {
            Stage("produce", "order-service", "orders", published, produce, "created", "published", "publish-failed"),
            Stage("log", "kafka", "orders", inLog, log, "in-log", "lost"),
            Stage("process", "order-processor", "orders", received, process, "received", "retry", "dead-lettered"),
            Stage("payment", "order-processor", "payments", paid, payment, "paid", "payment-failed"),
            Stage("analytics-order", "analytics", "orders", countedOrder, analyticsOrder, "counted-order"),
            Stage("analytics-payment", "analytics", "payments", countedPayment, analyticsPayment, "counted-payment"),
        };

        var statuses = stages.Select(s => s.Status).ToList();
        var verdict = steps.Count == 0 ? "unknown"
            : lost > 0 ? "lost"
            : statuses.Contains("duplicate") ? "duplicate"
            : dlq > 0 ? "dlq"
            : publishFailed > 0 && published == 0 ? "failed"
            : statuses.Contains("missing") ? "stuck"
            : statuses.Contains("failed") ? "failed"
            : statuses.All(s => s is "ok" or "skipped" or "unverified") ? "ok"
            : "in-progress";

        var business = lost > 0 ? "LOST" : dlq > 0 ? "DEAD_LETTERED" : publishFailed > 0 && published == 0 ? "FAILED"
            : countedPayment > 0 ? "REPORTED" : paid > 0 ? "PAID" : received > 0 ? "PROCESSING"
            : published > 0 ? "PUBLISHED" : created > 0 ? "CREATED" : "UNKNOWN";

        return new TraceView(cid, steps.Count > 0, verdict, business, steps.FirstOrDefault()?.Ts, steps.LastOrDefault()?.Ts, now,
            stages, steps, unreachable);
    }

    // ------------------------------------------------------------------ опрос сервисов

    /// <summary>GET /api/{path} у каждого работающего экземпляра трассируемых сервисов.</summary>
    private async Task<(List<JsonElement> Bodies, List<string> Unreachable)> FanOutAsync(string path, CancellationToken ct)
    {
        var unreachable = new List<string>();
        var calls = new List<Task<JsonElement?>>();
        foreach (var def in options.Services.Where(s => Traced.Contains(s.Name)))
        {
            foreach (var t in ServicePoller.Targets(def, monitor.Containers))
            {
                if (t.State != "running")
                {
                    unreachable.Add($"{t.Container} ({t.State})");
                    continue;
                }
                var target = t;
                calls.Add(Task.Run(async () =>
                {
                    try
                    {
                        using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        cts.CancelAfter(2500);
                        var http = httpFactory.CreateClient("services");
                        var json = await http.GetStringAsync($"http://{target.Host}:{def.Port}/api/{path}", cts.Token);
                        return (JsonElement?)JsonDocument.Parse(json).RootElement.Clone();
                    }
                    catch
                    {
                        lock (unreachable) unreachable.Add(target.Container);
                        return null;
                    }
                }, ct));
            }
        }
        var results = await Task.WhenAll(calls);
        return (results.Where(r => r is not null).Select(r => r!.Value).ToList(), unreachable);
    }

    private static T? Parse<T>(JsonElement e)
    {
        try { return e.Deserialize<T>(Json); } catch { return default; }
    }

    private static string? Str(JsonElement e, string name) =>
        e.TryGetProperty(name, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

    private static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}
