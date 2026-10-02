namespace OrderProcessor;

/// <summary>Держит N экземпляров ConsumerWorker в одной consumer group и позволяет добавлять/удалять/«ронять» их на лету.</summary>
public sealed class ConsumerManager(ProcessorContext ctx) : IHostedService
{
    private readonly List<ConsumerWorker> _workers = new();
    private readonly Lock _lock = new();
    private readonly long _startedAt = Environment.TickCount64;
    private int _nextId = 1;
    private long _historicDuplicates, _historicFailed, _historicRetries, _historicProcessed;

    public Task StartAsync(CancellationToken ct)
    {
        for (var i = 0; i < ctx.Kafka.InitialInstances; i++) Start(null);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken ct)
    {
        List<ConsumerWorker> all;
        lock (_lock) all = _workers.ToList();
        foreach (var w in all) w.StopGracefully();
        await Task.Run(() => { foreach (var w in all) w.Join(TimeSpan.FromSeconds(10)); }, ct);
        ctx.Dispose();
    }

    private ConsumerWorker? Find(int id)
    {
        lock (_lock) return _workers.FirstOrDefault(w => w.Id == id && !w.Finished);
    }

    private object Start(int? id)
    {
        lock (_lock)
        {
            var active = _workers.Count(w => !w.Finished);
            if (active >= ctx.Kafka.MaxInstances)
                return new { ok = false, error = $"Максимум {ctx.Kafka.MaxInstances} консюмеров", errorEn = $"At most {ctx.Kafka.MaxInstances} consumers" };
            var newId = id ?? _nextId;
            while (id is null && _workers.Any(w => w.Id == newId)) newId++;
            if (id is null) _nextId = newId + 1;

            var worker = new ConsumerWorker(newId, ctx);
            worker.Exited += OnExited;
            _workers.Add(worker);
            worker.Start();
            return new { ok = true, id = newId, clientId = worker.ClientId };
        }
    }

    private void OnExited(ConsumerWorker w)
    {
        // Даём UI секунду показать финальное состояние (stopped/crashed), потом убираем.
        _ = Task.Delay(1500).ContinueWith(_ =>
        {
            lock (_lock)
            {
                if (!_workers.Remove(w)) return;
                _historicDuplicates += w.Duplicates;
                _historicFailed += w.Failed;
                _historicRetries += w.Retries;
                _historicProcessed += w.Processed.Total;
            }
        });
    }

    public object Add() => Start(null);

    public object Remove(int id)
    {
        var w = Find(id);
        if (w is null) return new { ok = false, error = "нет такого консюмера", errorEn = "no such consumer" };
        w.StopGracefully();
        return new { ok = true };
    }

    public object Crash(int id)
    {
        var w = Find(id);
        if (w is null) return new { ok = false, error = "нет такого консюмера", errorEn = "no such consumer" };
        w.Crash();
        return new { ok = true };
    }

    public object Stuck(int id, bool? stuck)
    {
        var w = Find(id);
        if (w is null) return new { ok = false, error = "нет такого консюмера", errorEn = "no such consumer" };
        w.SetStuck(stuck ?? !w.IsStuck);
        return new { ok = true, stuck = w.IsStuck };
    }

    /// <summary>Падение и быстрый перезапуск с тем же client.id (и тем же group.instance.id, если включён static membership).</summary>
    public object Restart(int id)
    {
        var w = Find(id);
        if (w is null) return new { ok = false, error = "нет такого консюмера", errorEn = "no such consumer" };
        w.Crash();
        _ = Task.Run(async () =>
        {
            w.Join(TimeSpan.FromSeconds(10));
            await Task.Delay(2000);
            var isStatic = ctx.Settings.StaticMembership;
            ctx.Log.Add("info",
                $"processor-{id} перезапускается через 2 c после падения" +
                (isStatic ? " — static membership: координатор узнаёт его по group.instance.id, ребаланса не будет" : " — без static membership это новый участник → ребаланс"),
                $"processor-{id} restarts 2 s after the crash" +
                (isStatic ? " — static membership: the coordinator recognizes it by group.instance.id, no rebalance" : " — without static membership it's a new member → rebalance"),
                "static-membership");
            lock (_lock) _workers.Remove(w);
            Start(id);
        });
        return new { ok = true };
    }

    public object ApplySettings(SettingsPatch p)
    {
        var cur = ctx.Settings;
        var strategy = p.AssignmentStrategy is "range" or "roundrobin" or "cooperative-sticky" or "consumer" ? p.AssignmentStrategy : cur.AssignmentStrategy;
        var next = cur with
        {
            ProcessingDelayMs = Math.Clamp(p.ProcessingDelayMs ?? cur.ProcessingDelayMs, 0, 5000),
            FailureRatePercent = Math.Clamp(p.FailureRatePercent ?? cur.FailureRatePercent, 0, 100),
            MaxRetries = Math.Clamp(p.MaxRetries ?? cur.MaxRetries, 0, 10),
            AssignmentStrategy = strategy,
            SessionTimeoutMs = Math.Clamp(p.SessionTimeoutMs ?? cur.SessionTimeoutMs, 3000, 120000),
            MaxPollIntervalMs = Math.Clamp(p.MaxPollIntervalMs ?? cur.MaxPollIntervalMs, 3000, 600000),
            AutoCommitIntervalMs = Math.Clamp(p.AutoCommitIntervalMs ?? cur.AutoCommitIntervalMs, 100, 60000),
            StaticMembership = p.StaticMembership ?? cur.StaticMembership,
        };
        var notes = new List<string>();
        if (next.MaxPollIntervalMs < next.SessionTimeoutMs)
        {
            next = next with { MaxPollIntervalMs = next.SessionTimeoutMs };
            notes.Add("max.poll.interval.ms не может быть меньше session.timeout.ms");
        }

        var recreate = !next.SameConsumerConfig(cur);
        ctx.Settings = next;

        if (recreate)
        {
            List<ConsumerWorker> old;
            lock (_lock) old = _workers.Where(w => !w.Finished).ToList();
            var cfg = $"session.timeout={next.SessionTimeoutMs}, max.poll.interval={next.MaxPollIntervalMs}, " +
                      $"auto.commit.interval={next.AutoCommitIntervalMs}, static={next.StaticMembership}";
            ctx.Log.Add("info",
                $"Конфигурация консюмеров изменена (стратегия: {next.AssignmentStrategy}, {cfg}) — перезапускаем {old.Count} консюмеров",
                $"Consumer config changed (strategy: {next.AssignmentStrategy}, {cfg}) — restarting {old.Count} consumers",
                "consumer-group");
            _ = Task.Run(() =>
            {
                foreach (var w in old) w.StopGracefully();
                foreach (var w in old) w.Join(TimeSpan.FromSeconds(15));
                lock (_lock) foreach (var w in old) _workers.Remove(w);
                foreach (var w in old) Start(w.Id);
            });
        }

        return new { settings = next, recreated = recreate, notes };
    }

    public object GetStats()
    {
        List<ConsumerWorker> workers;
        long hd, hf, hr, hp;
        lock (_lock)
        {
            workers = _workers.OrderBy(w => w.Id).ToList();
            (hd, hf, hr, hp) = (_historicDuplicates, _historicFailed, _historicRetries, _historicProcessed);
        }

        return new
        {
            service = "order-processor",
            lang = "C#",
            role = "consumer",
            instanceId = Environment.MachineName,
            uptimeSec = (Environment.TickCount64 - _startedAt) / 1000,
            groupId = ctx.Kafka.GroupId,
            topic = ctx.Kafka.OrdersTopic,
            config = ctx.Settings,
            instances = workers.Select(w => w.GetStats()).ToList(),
            totals = new
            {
                processed = hp + workers.Sum(w => w.Processed.Total),
                rate = Math.Round(workers.Sum(w => w.Processed.Rate), 1),
                duplicates = hd + workers.Sum(w => w.Duplicates),
                failed = hf + workers.Sum(w => w.Failed),
                retries = hr + workers.Sum(w => w.Retries),
                dlq = ctx.DlqSent.Total,
                payments = new { sent = ctx.PaymentsSent.Total, rate = ctx.PaymentsSent.Rate, failed = ctx.PaymentsFailed.Total },
            },
            events = ctx.Log.Recent(),
        };
    }
}
