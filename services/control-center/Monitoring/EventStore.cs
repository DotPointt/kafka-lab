using System.Text.Json;

namespace ControlCenter.Monitoring;

/// <summary>Общая лента событий стенда (кластер + сервисы + действия пользователя).</summary>
public sealed class EventStore
{
    private readonly LinkedList<LabEvent> _events = new();
    private readonly Lock _lock = new();
    private long _nextId;

    public LabEvent Add(string level, string category, string text, string? learn = null, string? source = null, long? ts = null)
    {
        lock (_lock)
        {
            var e = new LabEvent(++_nextId, ts ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), level, category, text, learn, source);
            _events.AddLast(e);
            while (_events.Count > 2000) _events.RemoveFirst();
            return e;
        }
    }

    public List<LabEvent> Since(long id)
    {
        lock (_lock) return _events.Where(e => e.Id > id).ToList();
    }

    public List<LabEvent> Recent(int max)
    {
        lock (_lock) return _events.Skip(Math.Max(0, _events.Count - max)).ToList();
    }
}

/// <summary>Последний снимок + ожидание следующего (для SSE-стрима в браузер).</summary>
public sealed class SnapshotHub
{
    public static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly Lock _lock = new();
    private TaskCompletionSource _next = new(TaskCreationOptions.RunContinuationsAsynchronously);
    private long _version;

    public Snapshot? Latest { get; private set; }
    public string? LatestJson { get; private set; }

    public void Publish(Snapshot s)
    {
        var json = JsonSerializer.Serialize(s, Json);
        TaskCompletionSource toRelease;
        lock (_lock)
        {
            Latest = s;
            LatestJson = json;
            _version++;
            toRelease = _next;
            _next = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        }
        toRelease.TrySetResult();
    }

    public async Task<(string Json, long Version)> WaitNextAsync(long knownVersion, CancellationToken ct)
    {
        while (true)
        {
            Task wait;
            lock (_lock)
            {
                if (_version != knownVersion && LatestJson is not null) return (LatestJson, _version);
                wait = _next.Task;
            }
            await wait.WaitAsync(ct);
        }
    }
}
