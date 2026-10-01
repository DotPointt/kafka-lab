using System.Diagnostics;
using System.Text.Json;

namespace ControlCenter.Monitoring;

/// <summary>Снимок consumer group-ов в виде, удобном для передачи между процессами.</summary>
public sealed record ProbeOffset(string Topic, int Partition, long Offset);

public sealed record ProbeGroup(
    string Id, string State, string? Assignor, string? Type, int? Coordinator,
    List<MemberView> Members, List<ProbeOffset>? Committed, string? Error);

public sealed record ProbeResult(long Ts, List<ProbeGroup>? Groups, string? Error);

/// <summary>
/// Опрос consumer group-ов вынесен в ОТДЕЛЬНЫЙ ПРОЦЕСС.
///
/// Почему: admin-запросы к координатору групп в librdkafka (ListConsumerGroups, DescribeConsumerGroups,
/// ListConsumerGroupOffsets) в версиях ≤ 2.15 иногда роняют весь процесс (SIGSEGV, double free,
/// assert в rd_kafka_enq_once), если брокер останавливается посреди запроса. На учебном стенде брокеры
/// «падают» постоянно, поэтому падение изолировано: умер дочерний процесс — супервизор запускает его снова,
/// а control-center и UI продолжают работать (группы на секунду помечаются как устаревшие).
/// </summary>
public static class GroupProbe
{
    public const string Flag = "--group-probe";

    /// <summary>Точка входа дочернего процесса: раз в секунду пишет в stdout одну строку JSON.</summary>
    public static async Task RunChildAsync(LabOptions options)
    {
        // Родитель умер → stdin закрылся → выходим.
        _ = Task.Run(() =>
        {
            while (Console.In.ReadLine() is not null) { }
            Environment.Exit(0);
        });

        using var kafka = new KafkaInspector(options);
        var stdout = Console.Out;
        var badStreak = 0;
        while (true)
        {
            var started = Environment.TickCount64;
            ProbeResult result;
            try
            {
                var groups = await kafka.GroupsAsync();
                result = new ProbeResult(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), groups.Select(g => new ProbeGroup(
                    g.Id, g.State, g.Assignor, g.Type, g.Coordinator, g.Members,
                    g.Committed?.Select(kv => new ProbeOffset(kv.Key.Topic, kv.Key.Partition, kv.Value)).ToList(),
                    g.Error)).ToList(), null);
            }
            catch (Exception ex)
            {
                result = new ProbeResult(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), null, ex.Message);
            }
            await stdout.WriteLineAsync(JsonSerializer.Serialize(result, SnapshotHub.Json));
            await stdout.FlushAsync();

            // После перезапусков брокеров admin-клиент librdkafka иногда «залипает» на старом координаторе группы
            // (ошибка держится, хотя кластер уже здоров). Свежий процесс это лечит — выходим, супервизор перезапустит.
            badStreak = result.Error is not null || result.Groups!.Any(g => g.Error is not null) ? badStreak + 1 : 0;
            if (badStreak >= 8) Environment.Exit(3);
            var wait = 1000 - (int)(Environment.TickCount64 - started);
            if (wait > 0) await Task.Delay(wait);
        }
    }
}

/// <summary>Запускает дочерний процесс-зонд, читает его снимки и перезапускает при падении.</summary>
public sealed class GroupProbeSupervisor(ILogger<GroupProbeSupervisor> logger) : BackgroundService
{
    private volatile ProbeResult? _latest;
    private long _restarts;

    public ProbeResult? Latest => _latest;
    public long Restarts => Interlocked.Read(ref _restarts);

    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var self = Environment.ProcessPath!;
        var dll = typeof(GroupProbe).Assembly.Location;
        while (!ct.IsCancellationRequested)
        {
            using var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = self,
                    // в контейнере ProcessPath = dotnet, поэтому передаём путь к сборке
                    ArgumentList = { dll, GroupProbe.Flag },
                    RedirectStandardOutput = true,
                    RedirectStandardInput = true,
                    RedirectStandardError = true,
                    UseShellExecute = false,
                },
            };
            if (Path.GetFileNameWithoutExtension(self) != "dotnet") process.StartInfo.ArgumentList.RemoveAt(0);
            try
            {
                process.Start();
                process.ErrorDataReceived += (_, e) => { if (!string.IsNullOrEmpty(e.Data)) logger.LogDebug("probe: {Line}", e.Data); };
                process.BeginErrorReadLine();
                while (!ct.IsCancellationRequested && await process.StandardOutput.ReadLineAsync(ct) is { } line)
                {
                    try
                    {
                        _latest = JsonSerializer.Deserialize<ProbeResult>(line, SnapshotHub.Json);
                    }
                    catch (JsonException)
                    {
                        // посторонний вывод — пропускаем
                    }
                }
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "group probe failed");
            }
            finally
            {
                try { if (!process.HasExited) process.Kill(); } catch { }
            }
            if (ct.IsCancellationRequested) break;
            Interlocked.Increment(ref _restarts);
            logger.LogWarning("Процесс опроса consumer groups завершился (код {Code}) — перезапуск", SafeExitCode(process));
            await Task.Delay(500, ct);
        }
    }

    private static int? SafeExitCode(Process p)
    {
        try { return p.HasExited ? p.ExitCode : null; } catch { return null; }
    }
}
