using System.Collections.Concurrent;
using System.Globalization;
using ControlCenter.Docker;
using ControlCenter.Monitoring;

namespace ControlCenter.Chaos;

/// <summary>
/// Имитация сетевых проблем у брокера через Linux traffic control (`tc`) внутри его network namespace.
///
///   root qdisc: prio с 4 полосами
///     1:1 — весь трафик (сюда вешаем netem: задержка / потери / 100% потерь = полная изоляция)
///     1:4 — трафик к IP других брокеров (netem loss 100% = «split brain»: брокер не видит кластер, но клиенты видят его)
///
/// Правила применяются к ИСХОДЯЩЕМУ трафику брокера, но для TCP этого достаточно: без ответов соединение не живёт.
/// </summary>
public sealed class ChaosManager(DockerApi docker, EventStore events, LabOptions options)
{
    private sealed record Applied(NetworkChaos Chaos, string? StartedAt, string PeersKey);

    private readonly ConcurrentDictionary<int, Applied> _applied = new();
    private readonly SemaphoreSlim _gate = new(1, 1);

    public NetworkChaos Get(int brokerId) => _applied.TryGetValue(brokerId, out var a) ? a.Chaos : new NetworkChaos();

    public async Task<(bool Ok, string Message)> SetNetworkAsync(int brokerId, NetworkChaos chaos, IReadOnlyList<ContainerInfo> containers, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            return await ApplyAsync(brokerId, chaos, containers, announce: true, ct);
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<(bool Ok, string Message)> ApplyAsync(int brokerId, NetworkChaos chaos, IReadOnlyList<ContainerInfo> containers, bool announce, CancellationToken ct)
    {
        var def = options.Brokers.FirstOrDefault(b => b.Id == brokerId);
        var container = containers.FirstOrDefault(c => c.Name == def?.Container);
        if (def is null || container is null) return (false, $"Брокер {brokerId} не найден");
        if (container.State == "paused") return (false, "Контейнер на паузе — сначала сними паузу (tc выполняется внутри контейнера)");
        if (container.State != "running") return (false, "Контейнер остановлен — сеть настраивать не у кого");

        var peers = options.Brokers.Where(b => b.Id != brokerId)
            .Select(b => containers.FirstOrDefault(c => c.Name == b.Container)?.Ip)
            .Where(ip => !string.IsNullOrEmpty(ip)).Cast<string>().OrderBy(x => x).ToList();

        var script = BuildScript(chaos, peers);
        var result = await docker.ExecAsync(container.Id, ["sh", "-c", script], "root", null, ct);
        if (result.ExitCode != 0) return (false, $"tc завершился с кодом {result.ExitCode}: {result.StdErr.Trim()}");

        var startedAt = await docker.StartedAtAsync(container.Id, ct);
        if (chaos.IsNone) _applied.TryRemove(brokerId, out _);
        else _applied[brokerId] = new Applied(chaos, startedAt, string.Join(",", peers));

        if (announce) Announce(brokerId, chaos);
        return (true, result.StdOut.Trim());
    }

    private void Announce(int brokerId, NetworkChaos c)
    {
        if (c.IsNone)
        {
            events.Add("success", "chaos", $"Сеть брокера {brokerId} восстановлена (tc qdisc del)", "network-chaos");
            return;
        }
        var parts = new List<string>();
        if (c.Isolated) parts.Add("ПОЛНАЯ ИЗОЛЯЦИЯ (100% потерь)");
        else
        {
            if (c.LatencyMs > 0) parts.Add($"задержка {c.LatencyMs} мс ± {c.JitterMs} мс");
            if (c.LossPct > 0) parts.Add($"потеря {c.LossPct.ToString(CultureInfo.InvariantCulture)}% пакетов");
        }
        if (c.SplitFromBrokers && !c.Isolated) parts.Add("отрезан от других брокеров (клиенты его видят)");
        events.Add("warn", "chaos", $"Сеть брокера {brokerId}: {string.Join(", ", parts)}", c.SplitFromBrokers ? "split-brain" : "network-chaos");
    }

    private static string BuildScript(NetworkChaos c, List<string> peers)
    {
        var inv = CultureInfo.InvariantCulture;
        var lines = new List<string> { "tc qdisc del dev eth0 root 2>/dev/null || true" };
        if (!c.IsNone)
        {
            lines.Add("tc qdisc add dev eth0 root handle 1: prio bands 4 priomap 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0");
            if (c.Isolated)
            {
                lines.Add("tc qdisc add dev eth0 parent 1:1 handle 10: netem loss 100%");
            }
            else if (c.LatencyMs > 0 || c.LossPct > 0)
            {
                var netem = "netem";
                if (c.LatencyMs > 0) netem += $" delay {c.LatencyMs}ms" + (c.JitterMs > 0 ? $" {c.JitterMs}ms" : "");
                if (c.LossPct > 0) netem += $" loss {c.LossPct.ToString("0.##", inv)}%";
                netem += " limit 100000";
                lines.Add($"tc qdisc add dev eth0 parent 1:1 handle 10: {netem}");
            }
            if (c.SplitFromBrokers && !c.Isolated && peers.Count > 0)
            {
                lines.Add("tc qdisc add dev eth0 parent 1:4 handle 40: netem loss 100%");
                foreach (var ip in peers)
                    lines.Add($"tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst {ip}/32 flowid 1:4");
            }
        }
        lines.Add("tc qdisc show dev eth0");
        return "set -e\n" + string.Join("\n", lines);
    }

    /// <summary>
    /// Вызывается каждый тик монитора: если брокер перезапущен, tc-правила исчезли вместе с namespace — забываем их.
    /// Если у соседей сменились IP — переприменяем split.
    /// </summary>
    public async Task ReconcileAsync(IReadOnlyList<ContainerInfo> containers, CancellationToken ct)
    {
        if (_applied.IsEmpty || !await _gate.WaitAsync(0, ct)) return;
        try
        {
            foreach (var (brokerId, applied) in _applied.ToArray())
            {
                var def = options.Brokers.First(b => b.Id == brokerId);
                var container = containers.FirstOrDefault(c => c.Name == def.Container);
                if (container is null || container.State is "exited" or "dead" or "created")
                {
                    _applied.TryRemove(brokerId, out _);
                    events.Add("info", "chaos", $"Брокер {brokerId} остановлен — сетевые помехи сброшены вместе с его сетью", "network-chaos");
                    continue;
                }
                if (container.State != "running") continue;

                var startedAt = await docker.StartedAtAsync(container.Id, ct);
                if (startedAt != applied.StartedAt)
                {
                    _applied.TryRemove(brokerId, out _);
                    events.Add("info", "chaos", $"Брокер {brokerId} перезапущен — сетевые помехи сброшены", "network-chaos");
                    continue;
                }

                if (applied.Chaos.SplitFromBrokers)
                {
                    var peers = options.Brokers.Where(b => b.Id != brokerId)
                        .Select(b => containers.FirstOrDefault(c => c.Name == b.Container)?.Ip)
                        .Where(ip => !string.IsNullOrEmpty(ip)).Cast<string>().OrderBy(x => x);
                    if (string.Join(",", peers) != applied.PeersKey)
                        await ApplyAsync(brokerId, applied.Chaos, containers, announce: false, ct);
                }
            }
        }
        catch
        {
            // следующий тик попробует снова
        }
        finally
        {
            _gate.Release();
        }
    }
}
