using System.Text.Json;

namespace ControlCenter.Monitoring;

/// <summary>
/// Сравнивает два последовательных снимка и превращает разницу в человеческие события:
/// «сменился лидер», «ISR сократился», «начался ребаланс», «lag растёт»...
/// Ключ learn у события связывает его с объяснением в UI («Почему так?»).
/// </summary>
public sealed class EventDetector(EventStore store)
{
    private readonly Dictionary<string, long> _serviceEventIds = new();
    private readonly HashSet<string> _lagHigh = new();
    private long _firstTs;

    public void Detect(Snapshot? prev, Snapshot cur)
    {
        ImportServiceEvents(cur);
        if (_firstTs == 0) _firstTs = cur.Ts;
        if (prev is null) return;
        Brokers(prev, cur);
        Quorum(prev, cur);
        if (prev.Cluster.MetadataOk && cur.Cluster.MetadataOk) Partitions(prev, cur);
        if (prev.Cluster.GroupsOk && cur.Cluster.GroupsOk) Groups(prev, cur);
        Services(prev, cur);
    }

    // ------------------------------------------------------------------ брокеры

    private void Brokers(Snapshot prev, Snapshot cur)
    {
        foreach (var b in cur.Brokers)
        {
            var p = prev.Brokers.FirstOrDefault(x => x.Id == b.Id);
            if (p is null) continue;

            if (p.ContainerState != b.ContainerState && b.ContainerState != "unknown" && p.ContainerState != "unknown")
            {
                switch (b.ContainerState)
                {
                    case "exited" or "dead":
                        store.Add("error", "broker", $"Брокер {b.Id}: процесс остановлен ({b.ContainerStatus}). Его партиции-лидеры должны переехать на другие реплики из ISR", "broker-down");
                        break;
                    case "paused":
                        store.Add("warn", "broker", $"Брокер {b.Id} заморожен (docker pause): процесс не выполняется, но TCP-соединения открыты — как очень долгая GC-пауза", "broker-pause");
                        break;
                    case "running" when p.ContainerState == "paused":
                        store.Add("info", "broker", $"Брокер {b.Id} разморожен и продолжил работу с того места, где остановился", "broker-pause");
                        break;
                    case "running":
                        store.Add("success", "broker", $"Брокер {b.Id} запущен: восстанавливает лог с диска и догоняет лидеров, чтобы вернуться в ISR", "isr");
                        break;
                }
            }

            if (prev.Cluster.MetadataOk && cur.Cluster.MetadataOk && p.InMetadata != b.InMetadata)
            {
                if (!b.InMetadata && b.ContainerState is "running" or "paused")
                    store.Add("error", "broker", $"Контроллер зафенсил брокер {b.Id}: от него нет heartbeat дольше broker.session.timeout.ms (9 c). Брокер исключён из метаданных, лидерство переезжает", "broker-fencing");
                else if (!b.InMetadata)
                    store.Add("warn", "broker", $"Брокер {b.Id} исчез из метаданных кластера", "broker-fencing");
                else
                    store.Add("success", "broker", $"Брокер {b.Id} зарегистрирован в кластере (unfenced) — снова принимает реплики и лидерство", "broker-fencing");
            }
        }
    }

    private void Quorum(Snapshot prev, Snapshot cur)
    {
        var a = prev.Cluster.Quorum?.LeaderId;
        var b = cur.Cluster.Quorum?.LeaderId;
        if (a is not null && b is not null && a != b)
            store.Add("warn", "controller", $"KRaft: активный контроллер сменился — узел {a} → узел {b}. Кворум выбрал нового лидера метаданных (Raft)", "kraft");
        if (prev.Cluster.Quorum?.Error is null && cur.Cluster.Quorum?.Error is not null && a is not null)
            store.Add("error", "controller", $"KRaft: {cur.Cluster.Quorum.Error}", "kraft");
        if (prev.Cluster.Quorum?.Error is not null && cur.Cluster.Quorum?.Error is null && b is not null && a is null)
            store.Add("success", "controller", $"KRaft: кворум снова отвечает, активный контроллер — узел {b}", "kraft");
    }

    // ------------------------------------------------------------------ партиции

    private void Partitions(Snapshot prev, Snapshot cur)
    {
        var leaderMoves = new List<string>();
        var toPreferred = new List<string>();
        var offline = new List<string>();
        var online = new List<string>();
        var shrink = new List<string>();
        var expand = new List<string>();
        var underMin = new List<string>();
        var minOk = new List<string>();
        var coordinatorsMoved = 0;

        foreach (var t in cur.Topics)
        {
            var pt = prev.Topics.FirstOrDefault(x => x.Name == t.Name);
            if (pt is null)
            {
                if (!t.Internal) store.Add("info", "topic", $"Новый топик «{t.Name}»: {t.Partitions.Count} партиций, RF={t.ReplicationFactor}");
                continue;
            }
            if (!t.Internal && t.Partitions.Count > pt.Partitions.Count)
                store.Add("warn", "topic", $"Топик «{t.Name}»: партиций стало {t.Partitions.Count} (было {pt.Partitions.Count}). hash(key) % N изменился — новые сообщения с теми же ключами могут попасть в другие партиции!", "partitions-increase");

            var minIsr = int.TryParse(t.Config.GetValueOrDefault("min.insync.replicas"), out var m) ? m : 1;
            foreach (var p in t.Partitions)
            {
                var pp = pt.Partitions.FirstOrDefault(x => x.Id == p.Id);
                if (pp is null) continue;
                var name = $"{t.Name}-{p.Id}";

                if (t.Internal)
                {
                    if (pp.Leader != p.Leader) coordinatorsMoved++;
                    continue;
                }

                if (pp.Leader != p.Leader)
                {
                    if (p.Leader < 0) offline.Add(name);
                    else if (pp.Leader < 0) online.Add($"{name} (лидер {p.Leader})");
                    else if (p.Replicas.Length > 0 && p.Replicas[0] == p.Leader) toPreferred.Add($"{name} ({pp.Leader}→{p.Leader})");
                    else leaderMoves.Add($"{name} ({pp.Leader}→{p.Leader})");
                }

                var removed = pp.Isr.Except(p.Isr).ToArray();
                var added = p.Isr.Except(pp.Isr).ToArray();
                if (removed.Length > 0 && p.Leader >= 0) shrink.Add($"{name} −{string.Join(",", removed)}");
                if (added.Length > 0) expand.Add($"{name} +{string.Join(",", added)}");

                var wasUnder = pp.Leader >= 0 && pp.Isr.Length < minIsr;
                var isUnder = p.Leader >= 0 && p.Isr.Length < minIsr;
                if (isUnder && !wasUnder) underMin.Add($"{name} (ISR={p.Isr.Length}, min={minIsr})");
                if (!isUnder && wasUnder && p.Leader >= 0) minOk.Add(name);

                if (p.End is not null && pp.End is not null && p.End < pp.End)
                    store.Add("error", "partition", $"{name}: конец лога УМЕНЬШИЛСЯ {pp.End} → {p.End}. Новый лидер имеет более короткий лог — часть данных старого лидера отброшена (truncation)", "data-loss");
            }
        }

        if (offline.Count > 0)
            store.Add("error", "partition", $"Партиции OFFLINE — нет живого лидера среди ISR: {List(offline)}. Запись и чтение этих партиций невозможны", "offline-partition");
        if (online.Count > 0)
            store.Add("success", "partition", $"Партиции снова доступны: {List(online)}", "offline-partition");
        if (leaderMoves.Count > 0)
            store.Add("warn", "partition", $"Выборы лидера: {List(leaderMoves)}. Контроллер назначил нового лидера из ISR; клиенты обновят метаданные и переключатся", "leader-election");
        if (toPreferred.Count > 0)
            store.Add("info", "partition", $"Лидерство вернулось «предпочтительным» репликам (первая в списке replicas): {List(toPreferred)}", "preferred-leader");
        if (shrink.Count > 0)
            store.Add("warn", "partition", $"ISR сократился: {List(shrink)}. Реплика отстала дольше replica.lag.time.max.ms (10 c) или брокер недоступен", "isr");
        if (expand.Count > 0)
            store.Add("success", "partition", $"Реплики догнали лидера и вернулись в ISR: {List(expand)}", "isr");
        if (underMin.Count > 0)
            store.Add("error", "partition", $"ISR < min.insync.replicas: {List(underMin)}. Producer с acks=all получит NOT_ENOUGH_REPLICAS (а acks=1 продолжит писать — рискуя данными)", "min-isr");
        if (minOk.Count > 0)
            store.Add("success", "partition", $"ISR снова ≥ min.insync.replicas: {List(minOk)} — запись с acks=all возобновится", "min-isr");
        if (coordinatorsMoved > 0)
            store.Add("info", "partition", $"Сменились лидеры {coordinatorsMoved} партиций __consumer_offsets — координаторы части групп переехали на другие брокеры", "coordinator");
    }

    private static string List(List<string> items) =>
        items.Count <= 6 ? string.Join(", ", items) : string.Join(", ", items.Take(6)) + $" и ещё {items.Count - 6}";

    // ------------------------------------------------------------------ группы

    private void Groups(Snapshot prev, Snapshot cur)
    {
        // (_firstTs — момент первого снимка; нужен, чтобы не поднимать ложную тревогу по lag сразу после старта)
        foreach (var g in cur.Groups)
        {
            var pg = prev.Groups.FirstOrDefault(x => x.Id == g.Id);
            if (pg is null)
            {
                store.Add("info", "group", $"Появилась consumer group «{g.Id}» ({g.State})", "consumer-group");
                continue;
            }

            if (pg.State != g.State)
            {
                var (level, hint) = g.State switch
                {
                    "PreparingRebalance" => ("warn", " — координатор собирает участников (JoinGroup)"),
                    "CompletingRebalance" => ("warn", " — лидер группы раздаёт партиции (SyncGroup)"),
                    "Stable" => ("success", " — все участники получили партиции и читают"),
                    "Empty" => ("info", " — участников нет, но закоммиченные offset-ы сохранены"),
                    "Dead" => ("warn", " — группа удалена"),
                    _ => ("info", ""),
                };
                store.Add(level, "group", $"Группа «{g.Id}»: {pg.State} → {g.State}{hint}", "rebalance");
            }

            // Static-участник узнаётся по group.instance.id, даже если после рестарта у него новый member.id.
            static string Key(MemberView m) => m.InstanceId is null ? "m:" + m.MemberId : "i:" + m.InstanceId;
            var prevMembers = pg.Members.GroupBy(Key).ToDictionary(x => x.Key, x => x.First());
            var curMembers = g.Members.GroupBy(Key).ToDictionary(x => x.Key, x => x.First());
            var joined = g.Members.Where(m => !prevMembers.ContainsKey(Key(m))).Select(m => m.ClientId + (m.InstanceId is null ? "" : $" [static: {m.InstanceId}]")).ToList();
            var left = pg.Members.Where(m => !curMembers.ContainsKey(Key(m))).Select(m => m.ClientId).ToList();
            var restartedStatic = g.Members.Where(m => m.InstanceId is not null && prevMembers.TryGetValue(Key(m), out var pm) && pm.MemberId != m.MemberId)
                .Select(m => m.ClientId).ToList();
            if (restartedStatic.Count > 0)
                store.Add("success", "group", $"«{g.Id}»: {string.Join(", ", restartedStatic)} перезапустился, но координатор узнал его по group.instance.id — ребаланса нет, партиции те же (static membership)", "static-membership");
            if (joined.Count > 0) store.Add("info", "group", $"«{g.Id}»: вступили {string.Join(", ", joined)}", "consumer-group");
            if (left.Count > 0) store.Add("warn", "group", $"«{g.Id}»: покинули группу {string.Join(", ", left)}", "consumer-group");

            var moved = new List<string>();
            foreach (var m in g.Members)
            {
                if (!prevMembers.TryGetValue(Key(m), out var pm)) continue;
                var before = string.Join(",", pm.Assignment.Select(a => $"{a.Topic}-{a.Partition}"));
                var after = string.Join(",", m.Assignment.Select(a => $"{a.Topic}-{a.Partition}"));
                if (before != after) moved.Add($"{m.ClientId}: {Short(pm.Assignment)} → {Short(m.Assignment)}");
            }
            if (moved.Count > 0) store.Add("info", "group", $"«{g.Id}» перераспределила партиции: {string.Join("; ", moved)}", "rebalance");

            if (pg.Coordinator is not null && g.Coordinator is not null && pg.Coordinator != g.Coordinator)
                store.Add("warn", "group", $"Координатор группы «{g.Id}» переехал: брокер {pg.Coordinator} → {g.Coordinator} (сменился лидер её партиции в __consumer_offsets)", "coordinator");

            // Lag оцениваем во времени (lag / скорость чтения), иначе быстрый поток с редкими коммитами давал бы ложные тревоги.
            var lagSeconds = g.Rate > 1 ? g.TotalLag / g.Rate : (g.TotalLag > 0 ? double.PositiveInfinity : 0);
            var warmedUp = cur.Ts - _firstTs > 15_000; // скорость чтения считается по окну 10 c — сразу после старта её ещё нет
            if (warmedUp && g.TotalLag > 1000 && lagSeconds > 20 && _lagHigh.Add(g.Id))
                store.Add("warn", "group", $"Lag группы «{g.Id}» вырос до {N(g.TotalLag)} сообщений" +
                    (double.IsInfinity(lagSeconds) ? " — группа не читает" : $" (≈ {lagSeconds:F0} с отставания)") + " — консюмеры не успевают за продюсерами", "lag");
            else if ((g.TotalLag < 200 || lagSeconds < 5) && _lagHigh.Remove(g.Id))
                store.Add("success", "group", $"Группа «{g.Id}» догнала поток (lag {N(g.TotalLag)})", "lag");
        }
        foreach (var pg in prev.Groups.Where(pg => cur.Groups.All(g => g.Id != pg.Id)))
            store.Add("info", "group", $"Consumer group «{pg.Id}» больше не существует", "consumer-group");
    }

    private static string N(long v) => v.ToString("#,0", System.Globalization.CultureInfo.InvariantCulture).Replace(',', ' ');

    private static string Short(List<TopicPartitionRef> parts)
    {
        if (parts.Count == 0) return "∅";
        return string.Join(" ", parts.GroupBy(p => p.Topic).Select(g => $"{g.Key}[{string.Join(",", g.Select(p => p.Partition))}]"));
    }

    // ------------------------------------------------------------------ сервисы

    private void Services(Snapshot prev, Snapshot cur)
    {
        foreach (var s in cur.Services)
        {
            var ps = prev.Services.FirstOrDefault(x => x.Name == s.Name);
            if (ps is null) continue;
            foreach (var i in s.Instances)
            {
                var pi = ps.Instances.FirstOrDefault(x => x.Container == i.Container);
                if (pi is null)
                {
                    store.Add("info", "service", $"{i.Container}: новый экземпляр сервиса ({i.ContainerState})", null, s.Name);
                    continue;
                }
                if (pi.ContainerState == i.ContainerState) continue;
                var learn = s.Role == "consumer" ? "consumer-crash" : null;
                var (level, text) = i.ContainerState switch
                {
                    "exited" or "dead" => ("error", $"{i.Container} остановлен"),
                    "paused" => ("warn", $"{i.Container} заморожен (docker pause)"),
                    "running" when pi.ContainerState == "paused" => ("info", $"{i.Container} разморожен"),
                    "running" => ("success", $"{i.Container} запущен"),
                    _ => ("info", $"{i.Container}: {pi.ContainerState} → {i.ContainerState}"),
                };
                store.Add(level, "service", text, learn, s.Name);
            }
        }
    }

    private void ImportServiceEvents(Snapshot cur)
    {
        foreach (var s in cur.Services)
        {
            foreach (var i in s.Instances)
            {
                if (i.Stats is not { } stats || !stats.TryGetProperty("events", out var evs) || evs.ValueKind != JsonValueKind.Array) continue;
                var key = $"{s.Name}|{i.Container}";
                var last = _serviceEventIds.GetValueOrDefault(key);
                var maxId = 0L;
                var fresh = new List<JsonElement>();
                foreach (var e in evs.EnumerateArray())
                {
                    var id = e.GetProperty("id").GetInt64();
                    maxId = Math.Max(maxId, id);
                    if (id > last) fresh.Add(e);
                }
                if (maxId < last) // сервис перезапустился — нумерация началась заново
                {
                    fresh = evs.EnumerateArray().ToList();
                }
                foreach (var e in fresh)
                {
                    store.Add(
                        e.GetProperty("level").GetString() ?? "info",
                        "service",
                        e.GetProperty("text").GetString() ?? "",
                        e.TryGetProperty("learn", out var l) && l.ValueKind == JsonValueKind.String ? l.GetString() : null,
                        s.Instances.Count > 1 ? i.Container : s.Name,
                        e.TryGetProperty("ts", out var ts) ? ts.GetInt64() : null);
                }
                _serviceEventIds[key] = maxId;
            }
        }
    }
}
