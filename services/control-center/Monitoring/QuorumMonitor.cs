using ControlCenter.Docker;

namespace ControlCenter.Monitoring;

/// <summary>
/// Состояние KRaft-кворума контроллеров. Обычным клиентам активный контроллер не виден (они ходят только к брокерам),
/// поэтому спрашиваем штатной утилитой прямо внутри брокера:
///   kafka-metadata-quorum.sh --bootstrap-server localhost:9092 describe --replication
/// </summary>
public sealed class QuorumMonitor(DockerApi docker, SnapshotHub hub, LabOptions options, ILogger<QuorumMonitor> logger) : BackgroundService
{
    public QuorumInfo? Latest { get; private set; }
    private int _failures;

    /// <summary>Брокеры с сетевыми помехами, которых лучше не спрашивать (их ответ может быть устаревшим).</summary>
    public Func<int, bool> IsImpaired { get; set; } = _ => false;

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        if (!DockerApi.Available) return;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                await PollAsync(ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogDebug(ex, "quorum poll failed");
                Latest = new QuorumInfo { Error = ex.Message, UpdatedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), LeaderId = Latest?.LeaderId, Voters = Latest?.Voters ?? [] };
            }
            // JVM-утилита под нагрузкой стартует несколько секунд — запускаем следующую сразу после предыдущей
            await Task.Delay(TimeSpan.FromSeconds(2), ct);
        }
    }

    private async Task PollAsync(CancellationToken ct)
    {
        var snapshot = hub.Latest;
        var containers = await docker.ListAsync(options.ComposeProject, ct);
        var candidates = options.Brokers
            .Select(b => (Def: b, Container: containers.FirstOrDefault(c => c.Name == b.Container), View: snapshot?.Brokers.FirstOrDefault(x => x.Id == b.Id)))
            .Where(x => x.Container?.State == "running" && !IsImpaired(x.Def.Id))
            .OrderByDescending(x => x.View?.InMetadata ?? false)
            .ThenByDescending(x => x.Def.Id == Latest?.LeaderId)
            .ToList();
        if (candidates.Count == 0)
        {
            Latest = new QuorumInfo { Error = "Нет доступного брокера для запроса", UpdatedAt = Now };
            return;
        }

        var target = candidates[0];
        var result = await docker.ExecAsync(target.Container!.Id,
            ["timeout", "30", "/opt/kafka/bin/kafka-metadata-quorum.sh", "--bootstrap-server", "localhost:9092", "describe", "--replication"],
            "appuser",
            ["KAFKA_HEAP_OPTS=-Xmx96m", "KAFKA_JVM_PERFORMANCE_OPTS=-XX:+UseSerialGC -XX:TieredStopAtLevel=1"],
            ct);

        var voters = Parse(result.StdOut);
        if (voters.Count == 0)
        {
            // Единичный сбой (утилита не успела, брокер перезапускается) — оставляем последнее известное состояние.
            if (++_failures < 3 && Latest?.LeaderId is not null) return;
            Latest = new QuorumInfo
            {
                Error = "Кворум не ответил (нет лидера или нет большинства контроллеров)",
                UpdatedAt = Now,
                SourceBroker = target.Def.Id,
            };
            return;
        }
        _failures = 0;
        Latest = new QuorumInfo
        {
            LeaderId = voters.FirstOrDefault(v => v.Status == "Leader")?.Id,
            Voters = voters,
            UpdatedAt = Now,
            SourceBroker = target.Def.Id,
        };
    }

    private static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>
    /// NodeId  DirectoryId  LogEndOffset  Lag  LastFetchTimestamp  LastCaughtUpTimestamp  Status
    /// 3       AAAA...      177           0    1790875762734       1790875762734          Leader
    /// </summary>
    private static List<VoterView> Parse(string output)
    {
        var lines = output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var header = lines.FirstOrDefault(l => l.StartsWith("NodeId", StringComparison.Ordinal));
        if (header is null) return [];
        var cols = header.Split((char[])['\t', ' '], StringSplitOptions.RemoveEmptyEntries);
        int Col(string name) => Array.IndexOf(cols, name);
        var now = Now;
        var voters = new List<VoterView>();
        foreach (var line in lines.SkipWhile(l => l != header).Skip(1))
        {
            var v = line.Split((char[])['\t', ' '], StringSplitOptions.RemoveEmptyEntries);
            if (v.Length < cols.Length || !int.TryParse(v[Col("NodeId")], out var id)) continue;
            long L(string name) => Col(name) >= 0 && long.TryParse(v[Col(name)], out var x) ? x : -1;
            var lastFetch = L("LastFetchTimestamp");
            voters.Add(new VoterView(id, L("LogEndOffset"), L("Lag"), lastFetch > 0 ? Math.Max(0, now - lastFetch) : -1, v[Col("Status")]));
        }
        return voters.OrderBy(v => v.Id).ToList();
    }
}
