namespace OrderService.Infrastructure;

public sealed record LabEvent(long Id, long Ts, string Level, string Text, string? Learn);

/// <summary>
/// Журнал «интересных» событий сервиса (ошибки, ребалансы, потери...).
/// control-center забирает его вместе со /api/stats и показывает в общей ленте.
/// </summary>
public sealed class LabLog
{
    private readonly LinkedList<LabEvent> _events = new();
    private readonly Dictionary<string, long> _lastByKey = new();
    private readonly Lock _lock = new();
    private long _nextId;

    /// <param name="throttleKey">События с одинаковым ключом пишутся не чаще раза в <paramref name="throttleMs"/>.</param>
    public void Add(string level, string text, string? learn = null, string? throttleKey = null, int throttleMs = 5000)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        lock (_lock)
        {
            if (throttleKey is not null)
            {
                if (_lastByKey.TryGetValue(throttleKey, out var last) && now - last < throttleMs) return;
                _lastByKey[throttleKey] = now;
            }
            _events.AddLast(new LabEvent(++_nextId, now, level, text, learn));
            while (_events.Count > 100) _events.RemoveFirst();
        }
    }

    public List<LabEvent> Recent(int max = 50)
    {
        lock (_lock) return _events.Reverse().Take(max).Reverse().ToList();
    }
}
