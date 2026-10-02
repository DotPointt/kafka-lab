using Confluent.Kafka;
using OrderService.Infrastructure;

namespace OrderService;

/// <summary>
/// «Аудитор доставки». Отдельный consumer (без consumer group, через Assign) читает топик orders
/// и сверяет: всё ли, что брокер ПОДТВЕРДИЛ (ack), действительно лежит в логе по подтверждённому offset.
///
///  • confirmed  — подтверждённое сообщение прочитано по своему offset.
///  • lost       — по подтверждённому offset в логе оказалось ДРУГОЕ сообщение
///                 (лог был обрезан при смене лидера — типичная потеря при acks=1).
///  • duplicates — один и тот же orderId встретился в логе дважды (ретраи без идемпотентности).
/// </summary>
public sealed class DeliveryVerifier(KafkaOptions kafka, LabLog log) : BackgroundService
{
    private sealed record Pending(string OrderId, long At);
    private sealed record Seen(string OrderId, long At);

    private readonly Lock _lock = new();
    private readonly Dictionary<(int P, long O), Pending> _pending = new();   // ack пришёл, в логе ещё не видели
    private readonly Dictionary<(int P, long O), Seen> _seen = new();         // видели в логе, ack ещё не пришёл (или уже сверили)
    private readonly Queue<((int P, long O) Key, long At)> _seenOrder = new();
    private readonly Dictionary<string, (int P, long O, long At)> _orderIds = new();
    private readonly Queue<(string Id, long At)> _orderIdsOrder = new();
    private readonly Dictionary<int, long> _baseline = new();                 // с какого offset начали читать партицию
    private readonly Dictionary<int, long> _position = new();
    private readonly LinkedList<object> _lostSamples = new();
    private readonly LinkedList<object> _dupSamples = new();

    private long _confirmed, _lost, _unconfirmedTimeout, _duplicates, _unverifiable, _consumed;
    private volatile bool _running;
    private volatile string? _lastError;

    private static long Now => Environment.TickCount64;

    /// <summary>Вызывается из delivery report producer-а.</summary>
    public void OnAcked(int partition, long offset, string orderId)
    {
        if (offset < 0)
        {
            // acks=0: брокер ничего не отвечает, offset неизвестен — проверить нельзя.
            Interlocked.Increment(ref _unverifiable);
            return;
        }
        lock (_lock)
        {
            var key = (partition, offset);
            if (_seen.TryGetValue(key, out var seen))
            {
                Compare(key, orderId, seen.OrderId);
                return;
            }
            if (_baseline.TryGetValue(partition, out var b) && offset < b)
            {
                Interlocked.Increment(ref _unverifiable);
                return;
            }
            // Брокер уже подтверждал ДРУГОЙ заказ по этому же offset-у: значит, лог был обрезан и offset
            // переиспользован новым лидером. Первое подтверждение оказалось ложным — это потеря.
            if (_pending.TryGetValue(key, out var earlier) && earlier.OrderId != orderId)
                Compare(key, earlier.OrderId, orderId);
            _pending[key] = new Pending(orderId, Now);
        }
    }

    private void OnConsumed(int partition, long offset, string orderId)
    {
        lock (_lock)
        {
            _consumed++;
            _baseline.TryAdd(partition, offset);
            _position[partition] = offset + 1;
            var key = (partition, offset);

            if (_pending.Remove(key, out var pending)) Compare(key, pending.OrderId, orderId);
            _seen[key] = new Seen(orderId, Now);
            _seenOrder.Enqueue((key, Now));

            // один и тот же заказ в логе дважды = дубликат
            if (_orderIds.TryGetValue(orderId, out var prev) && (prev.P, prev.O) != key)
            {
                _duplicates++;
                AddSample(_dupSamples, new { orderId, first = $"{prev.P}@{prev.O}", second = $"{partition}@{offset}" });
                log.Add("warn",
                    $"Дубликат в логе: {orderId} записан дважды (orders-{prev.P}@{prev.O} и orders-{partition}@{offset}) — ретрай producer без идемпотентности",
                    $"Duplicate in the log: {orderId} was written twice (orders-{prev.P}@{prev.O} and orders-{partition}@{offset}) — a producer retry without idempotence",
                    "idempotence", "dup-in-log", 3000);
            }
            else
            {
                _orderIds[orderId] = (partition, offset, Now);
                _orderIdsOrder.Enqueue((orderId, Now));
            }
        }
    }

    private void Compare((int P, long O) key, string acked, string inLog)
    {
        if (acked == inLog)
        {
            _confirmed++;
            return;
        }
        _lost++;
        AddSample(_lostSamples, new { orderId = acked, partition = key.P, offset = key.O, replacedBy = inLog });
        log.Add("error",
            $"ПОТЕРЯ ДАННЫХ: заказ {acked} был подтверждён брокером (orders-{key.P}@{key.O}), но теперь по этому offset лежит {inLog}. " +
            "Лог старого лидера обрезали после выборов нового лидера.",
            $"DATA LOSS: order {acked} was acknowledged by the broker (orders-{key.P}@{key.O}), but that offset now holds {inLog}. " +
            "The old leader's log was truncated after a new leader was elected.", "data-loss", "lost", 3000);
    }

    private static void AddSample(LinkedList<object> list, object sample)
    {
        list.AddFirst(sample);
        while (list.Count > 10) list.RemoveLast();
    }

    private void Sweep()
    {
        var now = Now;
        lock (_lock)
        {
            foreach (var (key, p) in _pending.ToList())
            {
                if (_baseline.TryGetValue(key.P, out var b) && key.O < b)
                {
                    _pending.Remove(key);
                    Interlocked.Increment(ref _unverifiable);
                }
                else if (now - p.At > 120_000)
                {
                    // Подтверждён, но за 2 минуты так и не появился в логе.
                    _pending.Remove(key);
                    _unconfirmedTimeout++;
                    AddSample(_lostSamples, new { orderId = p.OrderId, partition = key.P, offset = key.O, replacedBy = (string?)null });
                    log.Add("error",
                        $"Заказ {p.OrderId} подтверждён (orders-{key.P}@{key.O}), но не найден в логе за 2 минуты — вероятно потерян",
                        $"Order {p.OrderId} was acknowledged (orders-{key.P}@{key.O}) but not found in the log within 2 minutes — probably lost",
                        "data-loss", "unconfirmed", 5000);
                }
            }
            while (_seenOrder.Count > 0 && now - _seenOrder.Peek().At > 90_000)
            {
                var (key, at) = _seenOrder.Dequeue();
                if (_seen.TryGetValue(key, out var s) && s.At == at) _seen.Remove(key);
            }
            while (_orderIdsOrder.Count > 0 && now - _orderIdsOrder.Peek().At > 90_000)
            {
                var (id, at) = _orderIdsOrder.Dequeue();
                if (_orderIds.TryGetValue(id, out var v) && v.At == at) _orderIds.Remove(id);
            }
        }
    }

    protected override Task ExecuteAsync(CancellationToken ct) =>
        Task.Factory.StartNew(() => Run(ct), ct, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private void Run(CancellationToken ct)
    {
        var lastSweep = Now;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
                {
                    BootstrapServers = kafka.BootstrapServers,
                    GroupId = "order-service-verifier", // нужен librdkafka, но мы не подписываемся и не коммитим
                    ClientId = "order-service-verifier",
                    EnableAutoCommit = false,
                    AutoOffsetReset = AutoOffsetReset.Latest,
                    FetchWaitMaxMs = 100,
                })
                .SetLogHandler((_, _) => { })
                .SetErrorHandler((_, _) => { })
                .Build();

                using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = kafka.BootstrapServers })
                    .SetLogHandler((_, _) => { }).Build();
                var md = admin.GetMetadata(kafka.OrdersTopic, TimeSpan.FromSeconds(10));
                var partitions = md.Topics.Single().Partitions
                    .Select(p => new TopicPartitionOffset(kafka.OrdersTopic, p.PartitionId, Offset.End)).ToList();
                if (partitions.Count == 0) throw new InvalidOperationException("Топик orders пока без партиций");

                lock (_lock) { _baseline.Clear(); _position.Clear(); _pending.Clear(); }
                consumer.Assign(partitions);
                _running = true;
                _lastError = null;

                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        var cr = consumer.Consume(250);
                        if (cr is { IsPartitionEOF: false, Message: not null })
                        {
                            var orderId = OrderIdOf(cr.Message);
                            if (orderId is not null) OnConsumed(cr.Partition.Value, cr.Offset.Value, orderId);
                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _lastError = ex.Error.Reason;
                    }
                    if (Now - lastSweep > 2000)
                    {
                        Sweep();
                        lastSweep = Now;
                    }
                }
            }
            catch (Exception ex) when (!ct.IsCancellationRequested)
            {
                _running = false;
                _lastError = ex.Message;
                Thread.Sleep(3000);
            }
        }
    }

    private static string? OrderIdOf(Message<string, string> m)
    {
        if (m.Headers is not null && m.Headers.TryGetLastBytes("order-id", out var bytes))
            return System.Text.Encoding.UTF8.GetString(bytes);
        return null;
    }

    public object GetStats()
    {
        lock (_lock)
        {
            return new
            {
                running = _running,
                lastError = _lastError,
                consumed = _consumed,
                pending = _pending.Count,
                confirmed = _confirmed,
                lost = _lost,
                unconfirmedTimeout = _unconfirmedTimeout,
                duplicatesInLog = _duplicates,
                unverifiable = Interlocked.Read(ref _unverifiable),
                lostSamples = _lostSamples.ToArray(),
                duplicateSamples = _dupSamples.ToArray(),
            };
        }
    }
}
