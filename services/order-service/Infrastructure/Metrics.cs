namespace OrderService.Infrastructure;

/// <summary>Счётчик событий + скорость (событий/сек) по последним секундам.</summary>
public sealed class RateCounter
{
    private const int Buckets = 8;
    private readonly long[] _buckets = new long[Buckets];
    private readonly Lock _lock = new();
    private long _total;
    private long _second = Environment.TickCount64 / 1000;

    public long Total => Interlocked.Read(ref _total);

    public void Add(long n = 1)
    {
        Interlocked.Add(ref _total, n);
        lock (_lock)
        {
            Advance();
            _buckets[_second % Buckets] += n;
        }
    }

    /// <summary>Средняя скорость за 3 последние полные секунды.</summary>
    public double Rate
    {
        get
        {
            lock (_lock)
            {
                Advance();
                long sum = 0;
                for (var i = 1; i <= 3; i++) sum += _buckets[(_second - i + Buckets) % Buckets];
                return Math.Round(sum / 3.0, 1);
            }
        }
    }

    private void Advance()
    {
        var now = Environment.TickCount64 / 1000;
        if (now == _second) return;
        var steps = Math.Min(now - _second, Buckets);
        for (var i = 1; i <= steps; i++) _buckets[(_second + i) % Buckets] = 0;
        _second = now;
    }
}

/// <summary>Скользящее окно задержек для перцентилей p50/p95/p99.</summary>
public sealed class LatencyTracker
{
    private readonly (long At, double Ms)[] _ring = new (long, double)[4096];
    private readonly Lock _lock = new();
    private int _next;
    private int _count;

    public void Record(double ms)
    {
        lock (_lock)
        {
            _ring[_next] = (Environment.TickCount64, ms);
            _next = (_next + 1) % _ring.Length;
            if (_count < _ring.Length) _count++;
        }
    }

    public object Snapshot(int windowMs = 5000)
    {
        double[] values;
        lock (_lock)
        {
            var since = Environment.TickCount64 - windowMs;
            values = _ring.Take(_count).Where(x => x.At >= since).Select(x => x.Ms).ToArray();
        }
        if (values.Length == 0) return new { p50 = (double?)null, p95 = (double?)null, p99 = (double?)null, max = (double?)null, samples = 0 };
        Array.Sort(values);
        double P(double q) => Math.Round(values[Math.Min(values.Length - 1, (int)(q * values.Length))], 1);
        return new { p50 = P(0.50), p95 = P(0.95), p99 = P(0.99), max = Math.Round(values[^1], 1), samples = values.Length };
    }
}
