using System.Diagnostics;
using Confluent.Kafka.Admin;
using ControlCenter.Chaos;
using ControlCenter.Docker;

namespace ControlCenter.Monitoring;

/// <summary>Раз в секунду собирает полный снимок стенда: Docker + Kafka + сервисы → события → браузер.</summary>
public sealed class ClusterMonitor(
    KafkaInspector kafka,
    DockerApi docker,
    ChaosManager chaos,
    QuorumMonitor quorum,
    GroupProbeSupervisor groupProbe,
    ServicePoller poller,
    SnapshotHub hub,
    EventStore events,
    LabOptions options,
    ILogger<ClusterMonitor> logger) : BackgroundService
{
    private readonly EventDetector _detector = new(events);
    private readonly Dictionary<(string, int), (long End, long At)> _endHistory = new();
    private readonly Dictionary<(string, int), double> _partitionRate = new();
    // История коммитов за ~10 c: консюмеры коммитят пачками (раз в auto.commit.interval), поэтому скорость
    // считаем по окну, а не по соседним тикам — иначе она «скачет» от 0 до тысяч.
    private readonly Dictionary<(string, string, int), Queue<(long At, long Committed)>> _commitHistory = new();
    private Snapshot? _prev;

    public IReadOnlyList<ContainerInfo>? Containers { get; private set; }

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        quorum.IsImpaired = id => !chaos.Get(id).IsNone;
        events.Add("info", "system", "control-center запущен: снимок кластера каждую секунду через AdminClient + Docker API",
            "control-center started: a cluster snapshot every second via AdminClient + Docker API");
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        do
        {
            try
            {
                await TickAsync(ct);
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                logger.LogWarning(ex, "Monitor tick failed");
            }
        } while (await timer.WaitForNextTickAsync(ct));
    }

    public async Task<IReadOnlyList<ContainerInfo>> RefreshContainersAsync(CancellationToken ct)
    {
        Containers = await docker.ListAsync(options.ComposeProject, ct);
        return Containers;
    }

    private static async Task<(T? Value, string? Error)> Safe<T>(Task<T> task)
    {
        try
        {
            return (await task, null);
        }
        catch (Exception ex)
        {
            return (default, ex.Message);
        }
    }

    private async Task TickAsync(CancellationToken ct)
    {
        var sw = Stopwatch.StartNew();
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var snap = new Snapshot { Ts = now };
        var prev = _prev;

        // 1) Docker: какие контейнеры живы
        List<ContainerInfo>? containers = null;
        if (DockerApi.Available)
        {
            var (list, err) = await Safe(docker.ListAsync(options.ComposeProject, ct));
            containers = list;
            snap.Cluster.DockerOk = list is not null;
            snap.Cluster.DockerError = err;
        }
        else
        {
            snap.Cluster.DockerError = "docker.sock is not mounted — chaos actions are unavailable / docker.sock не смонтирован — имитация сбоев недоступна";
        }
        Containers = containers;
        if (containers is not null) await chaos.ReconcileAsync(containers, ct);

        // 2) Метаданные кластера
        var (md, mdError) = await Safe(Task.Run(kafka.GetMetadata, ct));
        snap.Cluster.MetadataOk = md is not null;
        snap.Cluster.MetadataError = mdError;

        var topicsMeta = md?.Topics ?? prev?.Topics.Select(t => new TopicMeta(t.Name,
            t.Partitions.Select(p => new PartitionMeta(p.Id, p.Leader, p.Replicas, p.Isr, p.Error)).ToList(), null)).ToList() ?? [];
        var userPartitions = topicsMeta.Where(t => !KafkaInspector.IsInternal(t.Name))
            .SelectMany(t => t.Partitions.Select(p => (t.Name, p.Id))).ToList();

        // 3) Параллельно: offset-ы, сервисы, конфиги
        var latestTask = Safe(kafka.ListOffsetsAsync(userPartitions, OffsetSpec.Latest()));
        var earliestTask = Safe(kafka.ListOffsetsAsync(userPartitions, OffsetSpec.Earliest()));
        var servicesTask = Safe(poller.PollAsync(containers, ct));
        var configsTask = Safe(kafka.TopicConfigsAsync(topicsMeta.Select(t => t.Name)));
        var clusterIdTask = Safe(kafka.ClusterIdAsync());
        await Task.WhenAll(latestTask, earliestTask, servicesTask, configsTask, clusterIdTask);

        var (latest, latestErr) = latestTask.Result;
        var (earliest, _) = earliestTask.Result;
        // Группы приходят из отдельного процесса-зонда (см. GroupProbe.cs); снимок старше 4 c считаем устаревшим.
        var probe = groupProbe.Latest;
        var probeFresh = probe?.Groups is not null && now - probe.Ts < 4000;
        var groups = probeFresh ? probe!.Groups!.Select(g => new GroupResult(g.Id, g.State, g.Assignor, g.Type, g.Coordinator, g.Members,
            g.Committed?.ToDictionary(o => (o.Topic, o.Partition), o => o.Offset), g.Error)).ToList() : null;
        var groupsErr = probeFresh ? null : probe?.Error ?? "нет свежих данных о группах";
        var configs = configsTask.Result.Value ?? new();
        snap.Cluster.OffsetsOk = latest is not null && latestErr is null;
        snap.Cluster.GroupsOk = groups is not null && groupsErr is null;
        snap.Cluster.ClusterId = clusterIdTask.Result.Value;
        snap.Services = servicesTask.Result.Value ?? prev?.Services ?? [];

        // 4) Топики и партиции
        foreach (var t in topicsMeta)
        {
            var prevTopic = prev?.Topics.FirstOrDefault(x => x.Name == t.Name);
            var tv = new TopicView
            {
                Name = t.Name,
                Internal = KafkaInspector.IsInternal(t.Name),
                ReplicationFactor = t.Partitions.Count == 0 ? 0 : t.Partitions.Max(p => p.Replicas.Length),
                Config = configs.GetValueOrDefault(t.Name) ?? prevTopic?.Config ?? [],
            };
            foreach (var p in t.Partitions)
            {
                var key = (t.Name, p.Id);
                var prevPart = prevTopic?.Partitions.FirstOrDefault(x => x.Id == p.Id);
                var pv = new PartitionView
                {
                    Id = p.Id, Leader = p.Leader, Replicas = p.Replicas, Isr = p.Isr, Error = p.Error,
                    End = latest is not null && latest.TryGetValue(key, out var e) ? e : prevPart?.End,
                    Start = earliest is not null && earliest.TryGetValue(key, out var s) ? s : prevPart?.Start,
                };
                if (latest is not null && latest.TryGetValue(key, out var end))
                {
                    var rate = _partitionRate.GetValueOrDefault(key);
                    if (_endHistory.TryGetValue(key, out var h) && now > h.At)
                    {
                        var instant = Math.Max(0, (end - h.End) * 1000.0 / (now - h.At));
                        rate = rate * 0.4 + instant * 0.6;
                    }
                    _endHistory[key] = (end, now);
                    _partitionRate[key] = rate;
                    pv.Rate = Math.Round(rate, 1);
                }
                tv.Partitions.Add(pv);
            }
            tv.Rate = Math.Round(tv.Partitions.Sum(p => p.Rate), 1);
            snap.Topics.Add(tv);
        }

        // 5) Брокеры
        var quorumInfo = quorum.Latest;
        foreach (var def in options.Brokers)
        {
            var c = containers?.FirstOrDefault(x => x.Name == def.Container);
            var prevBroker = prev?.Brokers.FirstOrDefault(x => x.Id == def.Id);
            var userTopics = snap.Topics.Where(t => !t.Internal).ToList();
            var bv = new BrokerView
            {
                Id = def.Id,
                Container = def.Container,
                ContainerState = c?.State ?? (containers is null ? "unknown" : "missing"),
                ContainerStatus = c?.Status,
                Ip = c?.Ip,
                InMetadata = md is not null ? md.Brokers.Contains(def.Id) : prevBroker?.InMetadata ?? false,
                Chaos = chaos.Get(def.Id),
                Leaders = userTopics.Sum(t => t.Partitions.Count(p => p.Leader == def.Id)),
                Replicas = userTopics.Sum(t => t.Partitions.Count(p => p.Replicas.Contains(def.Id))),
                InRate = Math.Round(userTopics.Sum(t => t.Partitions.Where(p => p.Leader == def.Id).Sum(p => p.Rate)), 1),
                IsQuorumLeader = quorumInfo?.LeaderId == def.Id,
                Voter = quorumInfo?.Voters.FirstOrDefault(v => v.Id == def.Id),
            };
            bv.Status = bv.ContainerState switch
            {
                "paused" => "paused",
                "exited" or "dead" or "created" or "missing" => "stopped",
                "restarting" => "starting",
                "running" when bv.InMetadata => "online",
                "running" when (c?.Status ?? "").Contains("health: starting") => "starting",
                "running" => "fenced",
                _ => bv.InMetadata ? "online" : "unknown",
            };
            snap.Brokers.Add(bv);
        }

        // 6) Consumer groups и lag
        var groupResults = groups ?? [];
        foreach (var g in groupResults)
        {
            var prevGroup = prev?.Groups.FirstOrDefault(x => x.Id == g.Id);
            var gv = new GroupView
            {
                Id = g.Id, State = g.State, Assignor = g.Assignor, Type = g.Type, Coordinator = g.Coordinator,
                Members = g.Members, Error = g.Error,
            };
            var groupTopics = new HashSet<string>(g.Members.SelectMany(m => m.Assignment.Select(a => a.Topic)));
            if (g.Committed is not null) groupTopics.UnionWith(g.Committed.Keys.Select(k => k.Topic));
            else if (prevGroup is not null) groupTopics.UnionWith(prevGroup.Offsets.Select(o => o.Topic));

            foreach (var t in snap.Topics.Where(t => groupTopics.Contains(t.Name)))
            {
                foreach (var p in t.Partitions)
                {
                    long? committed = g.Committed is not null
                        ? (g.Committed.TryGetValue((t.Name, p.Id), out var c) ? c : null)
                        : prevGroup?.Offsets.FirstOrDefault(o => o.Topic == t.Name && o.Partition == p.Id)?.Committed;
                    var ov = new GroupOffsetView { Topic = t.Name, Partition = p.Id, Committed = committed };
                    if (committed is not null && p.End is not null) ov.Lag = Math.Max(0, p.End.Value - committed.Value);

                    var key = (g.Id, t.Name, p.Id);
                    if (committed is not null)
                    {
                        if (!_commitHistory.TryGetValue(key, out var hist)) _commitHistory[key] = hist = new Queue<(long, long)>();
                        hist.Enqueue((now, committed.Value));
                        while (hist.Count > 2 && now - hist.Peek().At > 10_000) hist.Dequeue();
                        var (oldAt, oldCommitted) = hist.Peek();
                        ov.Rate = now > oldAt ? Math.Round(Math.Max(0, (committed.Value - oldCommitted) * 1000.0 / (now - oldAt)), 1) : 0;
                    }
                    gv.Offsets.Add(ov);
                }
            }
            gv.TotalLag = gv.Offsets.Sum(o => o.Lag ?? 0);
            gv.Rate = Math.Round(gv.Offsets.Sum(o => o.Rate), 1);
            snap.Groups.Add(gv);
        }
        if (groups is null && prev is not null) snap.Groups = prev.Groups;

        // 7) Сводка
        var user = snap.Topics.Where(t => !t.Internal).ToList();
        snap.Cluster.BrokersTotal = options.Brokers.Count;
        snap.Cluster.BrokersAlive = snap.Brokers.Count(b => b.InMetadata);
        snap.Cluster.UnderReplicated = user.Sum(t => t.Partitions.Count(p => p.Leader >= 0 && p.Isr.Length < p.Replicas.Length));
        snap.Cluster.Offline = user.Sum(t => t.Partitions.Count(p => p.Leader < 0));
        snap.Cluster.UnderMinIsr = user.Sum(t =>
        {
            var min = int.TryParse(t.Config.GetValueOrDefault("min.insync.replicas"), out var m) ? m : 1;
            return t.Partitions.Count(p => p.Leader >= 0 && p.Isr.Length < min);
        });
        snap.Cluster.ProduceRate = Math.Round(user.Sum(t => t.Rate), 1);
        snap.Cluster.Quorum = quorumInfo;
        snap.Cluster.TickMs = sw.ElapsedMilliseconds;

        _detector.Detect(prev, snap);
        hub.Publish(snap);
        _prev = snap;
    }
}
