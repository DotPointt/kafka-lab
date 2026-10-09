using System.Text.RegularExpressions;

namespace OrderService.Infrastructure;

/// <summary>
/// Один шаг бизнес-транзакции: что сервис сделал с заказом и в какое бизнес-состояние его перевёл.
/// Шаги разных сервисов связываются через correlation id (заголовок Kafka «correlation-id»).
/// </summary>
public sealed record TraceStep(long Ts, string Service, string Instance, string Step, string State,
    string? Topic = null, int? Partition = null, long? Offset = null, string? Detail = null);

/// <summary>Аномалия по correlation id (дубль, потеря, DLQ...) — для списка «Проблемы» в UI.</summary>
public sealed record TraceAnomaly(long Ts, string Cid, string Kind, string Instance, string? Detail);

/// <summary>Недавно созданная бизнес-транзакция (заказ).</summary>
public sealed record TraceRecent(long Ts, string Cid, string OrderId, string Customer, bool Manual);

/// <summary>
/// Журнал трассировки: correlation id → шаги, которые ЭТОТ сервис выполнил по транзакции.
/// Ограничен по размеру (старые id вытесняются), чтобы не расти бесконечно под нагрузкой.
/// control-center опрашивает такие журналы всех сервисов и собирает полный путь события.
/// </summary>
public sealed partial class TraceLog(string service)
{
    private const int Capacity = 50_000, MaxStepsPerId = 40, MaxAnomalies = 60, MaxRecent = 20;

    private readonly Lock _lock = new();
    private readonly Dictionary<string, List<TraceStep>> _steps = new();
    private readonly Queue<string> _order = new();
    private readonly LinkedList<TraceAnomaly> _anomalies = new();
    private readonly LinkedList<TraceRecent> _recent = new();       // сгенерированные заказы
    private readonly LinkedList<TraceRecent> _recentManual = new(); // отправленные вручную: их не вытесняет поток генератора

    public const string Header = "correlation-id";

    public static long Now => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>Новый correlation id: «c-» + 8 hex-символов.</summary>
    public static string NewId() => "c-" + Guid.NewGuid().ToString("N")[..8];

    /// <summary>Correlation id, заданный пользователем, допускается только из безопасных символов.</summary>
    public static string? Sanitize(string? cid)
    {
        if (string.IsNullOrWhiteSpace(cid)) return null;
        cid = cid.Trim();
        return cid.Length <= 64 && SafeId().IsMatch(cid) ? cid : null;
    }

    /// <summary>Записать шаг. Возвращает, который раз по этому id выполняется такой шаг (1 — первый).</summary>
    public int Add(string? cid, string instance, string step, string state,
        string? topic = null, int? partition = null, long? offset = null, string? detail = null)
    {
        if (string.IsNullOrEmpty(cid)) return 0;
        lock (_lock)
        {
            if (!_steps.TryGetValue(cid, out var list))
            {
                list = [];
                _steps[cid] = list;
                _order.Enqueue(cid);
                while (_order.Count > Capacity) _steps.Remove(_order.Dequeue());
            }
            if (list.Count < MaxStepsPerId)
                list.Add(new TraceStep(Now, service, instance, step, state, topic, partition, offset, detail));
            return list.Count(s => s.Step == step);
        }
    }

    /// <summary>Сколько раз по id уже выполнялся шаг.</summary>
    public int Count(string? cid, string step)
    {
        if (string.IsNullOrEmpty(cid)) return 0;
        lock (_lock) return _steps.TryGetValue(cid, out var list) ? list.Count(s => s.Step == step) : 0;
    }

    public void Anomaly(string? cid, string kind, string instance, string? detail = null)
    {
        if (string.IsNullOrEmpty(cid)) return;
        lock (_lock)
        {
            _anomalies.AddFirst(new TraceAnomaly(Now, cid, kind, instance, detail));
            while (_anomalies.Count > MaxAnomalies) _anomalies.RemoveLast();
        }
    }

    public void Recent(string cid, string orderId, string customer, bool manual)
    {
        lock (_lock)
        {
            var list = manual ? _recentManual : _recent;
            list.AddFirst(new TraceRecent(Now, cid, orderId, customer, manual));
            while (list.Count > MaxRecent) list.RemoveLast();
        }
    }

    public List<TraceStep> Get(string cid)
    {
        lock (_lock) return _steps.TryGetValue(cid, out var list) ? list.ToList() : [];
    }

    /// <summary>Шаги сразу по нескольким id (ids — через запятую).</summary>
    public Dictionary<string, List<TraceStep>> GetMany(string? ids)
    {
        var result = new Dictionary<string, List<TraceStep>>();
        if (string.IsNullOrEmpty(ids)) return result;
        lock (_lock)
            foreach (var id in ids.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).Distinct().Take(100))
                if (_steps.TryGetValue(id, out var list)) result[id] = list.ToList();
        return result;
    }

    public object RecentView()
    {
        lock (_lock) return new { service, recent = _recentManual.Concat(_recent).ToArray(), anomalies = _anomalies.ToArray() };
    }

    [GeneratedRegex("^[A-Za-z0-9._:-]+$")]
    private static partial Regex SafeId();
}
