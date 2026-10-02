using System.Text.Json;
using Confluent.Kafka;
using OrderProcessor.Infrastructure;

namespace OrderProcessor;

/// <summary>
/// Один экземпляр консюмера в группе order-processing. Работает в своём потоке:
///   Consume() → обработка → produce в payments → StoreOffset() → (librdkafka раз в N мс коммитит offset).
/// Это at-least-once: offset сохраняется ПОСЛЕ обработки, поэтому при крэше часть сообщений обработается повторно.
/// </summary>
public sealed class ConsumerWorker
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly ProcessorContext _ctx;
    private readonly ProcessorSettings _consumerSettings; // снимок настроек на момент создания
    private readonly Thread _thread;
    private readonly Lock _lock = new();
    private readonly SortedSet<int> _assigned = new();
    private readonly Dictionary<int, long> _positions = new();

    private volatile bool _stop, _crash, _stuck;
    private volatile string _state = "starting";
    private volatile string? _memberId, _lastError;
    private long _failed, _retries, _dlq, _duplicates, _rebalances, _stuckSince;

    public int Id { get; }
    public string ClientId => $"processor-{Id}";
    public RateCounter Processed { get; } = new();
    public string State => _state;
    public bool Finished { get; private set; }
    public event Action<ConsumerWorker>? Exited;

    public ConsumerWorker(int id, ProcessorContext ctx)
    {
        Id = id;
        _ctx = ctx;
        _consumerSettings = ctx.Settings;
        _thread = new Thread(Run) { IsBackground = true, Name = ClientId };
    }

    public void Start() => _thread.Start();

    /// <summary>Корректное завершение: Close() → коммит offset-ов + LeaveGroup → мгновенный ребаланс.</summary>
    public void StopGracefully()
    {
        _state = "closing";
        _stop = true;
    }

    /// <summary>Имитация падения процесса: без коммита и без LeaveGroup. Группа узнает только по session.timeout.</summary>
    public void Crash()
    {
        _state = "crashing";
        _crash = true;
        _stop = true;
    }

    /// <summary>Имитация зависания: поток не вызывает Consume(). Heartbeat-ы продолжаются (их шлёт фоновый поток librdkafka),
    /// но через max.poll.interval.ms консюмер сам покинет группу.</summary>
    public void SetStuck(bool stuck)
    {
        _stuck = stuck;
        _stuckSince = stuck ? Environment.TickCount64 : 0;
        _ctx.Log.Add(stuck ? "warn" : "info",
            stuck
                ? $"{ClientId} завис (не вызывает Consume). Через max.poll.interval.ms={_consumerSettings.MaxPollIntervalMs} мс он сам выйдет из группы"
                : $"{ClientId} отвис и продолжает Consume()",
            stuck
                ? $"{ClientId} is stuck (doesn't call Consume). After max.poll.interval.ms={_consumerSettings.MaxPollIntervalMs} ms it leaves the group by itself"
                : $"{ClientId} is unstuck and calls Consume() again",
            "max-poll-interval");
    }

    public bool IsStuck => _stuck;

    public void Join(TimeSpan timeout) => _thread.Join(timeout);

    private ConsumerConfig BuildConfig()
    {
        var s = _consumerSettings;
        var config = new ConsumerConfig
        {
            BootstrapServers = _ctx.Kafka.BootstrapServers,
            GroupId = _ctx.Kafka.GroupId,
            ClientId = ClientId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            // Паттерн at-least-once для librdkafka: автокоммит включён, но offset попадает в «кандидаты на коммит»
            // только когда мы явно вызовем StoreOffset() после успешной обработки.
            EnableAutoCommit = true,
            EnableAutoOffsetStore = false,
            AutoCommitIntervalMs = s.AutoCommitIntervalMs,
            MaxPollIntervalMs = s.MaxPollIntervalMs,
            FetchWaitMaxMs = 100,
            GroupInstanceId = s.StaticMembership ? ClientId : null,
        };

        if (s.AssignmentStrategy == "consumer")
        {
            // KIP-848: назначение партиций вычисляет координатор на брокере, ребаланс инкрементальный,
            // session.timeout / heartbeat задаются на брокере (group.consumer.*).
            config.GroupProtocol = GroupProtocol.Consumer;
        }
        else
        {
            config.GroupProtocol = GroupProtocol.Classic;
            config.SessionTimeoutMs = s.SessionTimeoutMs;
            config.HeartbeatIntervalMs = Math.Max(500, s.SessionTimeoutMs / 4);
            config.PartitionAssignmentStrategy = s.AssignmentStrategy switch
            {
                "range" => PartitionAssignmentStrategy.Range,
                "roundrobin" => PartitionAssignmentStrategy.RoundRobin,
                _ => PartitionAssignmentStrategy.CooperativeSticky,
            };
        }
        return config;
    }

    private void Run()
    {
        IConsumer<string, string>? consumer = null;
        try
        {
            consumer = new ConsumerBuilder<string, string>(BuildConfig())
                .SetPartitionsAssignedHandler((c, parts) =>
                {
                    lock (_lock) foreach (var p in parts) _assigned.Add(p.Partition.Value);
                    Interlocked.Increment(ref _rebalances);
                    _memberId = c.MemberId;
                    if (parts.Count > 0)
                        _ctx.Log.Add("info",
                            $"{ClientId}: назначены партиции [{Fmt(parts)}] → теперь [{AssignedText()}]",
                            $"{ClientId}: assigned partitions [{Fmt(parts)}] → now [{AssignedText()}]", "rebalance");
                })
                .SetPartitionsRevokedHandler((_, parts) =>
                {
                    lock (_lock) foreach (var p in parts) { _assigned.Remove(p.Partition.Value); _positions.Remove(p.Partition.Value); }
                    _ctx.Log.Add("info",
                        $"{ClientId}: отозваны партиции [{Fmt(parts.Select(p => p.TopicPartition))}] (offset-ы коммитятся перед отдачей)",
                        $"{ClientId}: revoked partitions [{Fmt(parts.Select(p => p.TopicPartition))}] (offsets are committed before handing them over)", "rebalance");
                })
                .SetPartitionsLostHandler((_, parts) =>
                {
                    lock (_lock) foreach (var p in parts) { _assigned.Remove(p.Partition.Value); _positions.Remove(p.Partition.Value); }
                    _ctx.Log.Add("warn",
                        $"{ClientId}: партиции ПОТЕРЯНЫ [{Fmt(parts.Select(p => p.TopicPartition))}] — консюмер выкинут из группы, коммит невозможен",
                        $"{ClientId}: partitions LOST [{Fmt(parts.Select(p => p.TopicPartition))}] — the consumer was kicked out of the group, it can't commit", "max-poll-interval");
                })
                .SetErrorHandler((_, e) => { _lastError = e.Reason; })
                .SetLogHandler((_, m) =>
                {
                    if (m.Message.Contains("max.poll.interval.ms", StringComparison.OrdinalIgnoreCase))
                        _ctx.Log.Add("warn", $"{ClientId}: {m.Message}", $"{ClientId}: {m.Message}", "max-poll-interval", "maxpoll-" + Id, 3000);
                })
                .Build();

            consumer.Subscribe(_ctx.Kafka.OrdersTopic);
            _state = "running";
            var mode = _consumerSettings.AssignmentStrategy + (_consumerSettings.StaticMembership ? ", static membership" : "");
            _ctx.Log.Add("info",
                $"{ClientId} запущен и подписался на {_ctx.Kafka.OrdersTopic} (стратегия: {mode})",
                $"{ClientId} started and subscribed to {_ctx.Kafka.OrdersTopic} (strategy: {mode})", "consumer-group");

            while (!_stop)
            {
                if (_stuck)
                {
                    _state = "stuck";
                    Thread.Sleep(100);
                    continue;
                }
                _state = "running";

                ConsumeResult<string, string>? cr;
                try
                {
                    cr = consumer.Consume(200);
                }
                catch (ConsumeException ex)
                {
                    _lastError = ex.Error.Reason;
                    if (ex.Error.Code == ErrorCode.Local_MaxPollExceeded)
                        _ctx.Log.Add("warn",
                            $"{ClientId}: превышен max.poll.interval.ms — консюмер покинул группу и вступит заново",
                            $"{ClientId}: max.poll.interval.ms exceeded — the consumer left the group and will rejoin", "max-poll-interval");
                    continue;
                }
                if (cr?.Message is null) continue;

                Process(cr);

                try
                {
                    consumer.StoreOffset(cr); // отметить «обработано до cr.Offset включительно» → закоммитится через AutoCommitIntervalMs
                }
                catch (KafkaException)
                {
                    // партицию уже отобрали при ребалансе — это нормально
                }
            }

            if (_crash)
            {
                _state = "crashed";
                _ctx.Log.Add("error",
                    $"{ClientId} УПАЛ (имитация kill -9): offset-ы последних {_consumerSettings.AutoCommitIntervalMs} мс не закоммичены, " +
                    $"LeaveGroup не отправлен. Координатор заметит пропажу через session.timeout.ms={_consumerSettings.SessionTimeoutMs} мс",
                    $"{ClientId} CRASHED (simulated kill -9): offsets of the last {_consumerSettings.AutoCommitIntervalMs} ms are not committed, " +
                    $"no LeaveGroup sent. The coordinator notices only after session.timeout.ms={_consumerSettings.SessionTimeoutMs} ms", "consumer-crash");
                // Dispose() без Close(): librdkafka уничтожается без commit и без LeaveGroup.
                consumer.Dispose();
            }
            else
            {
                consumer.Close(); // commit сохранённых offset-ов + LeaveGroup
                consumer.Dispose();
                _state = "stopped";
                _ctx.Log.Add("info",
                    $"{ClientId} корректно остановлен: offset-ы закоммичены, LeaveGroup → ребаланс сразу",
                    $"{ClientId} stopped gracefully: offsets committed, LeaveGroup → immediate rebalance", "rebalance");
            }
        }
        catch (Exception ex)
        {
            _state = "failed";
            _lastError = ex.Message;
            _ctx.Log.Add("error", $"{ClientId}: {ex.Message}", $"{ClientId}: {ex.Message}");
            try { consumer?.Dispose(); } catch { }
        }
        finally
        {
            lock (_lock) { _assigned.Clear(); _positions.Clear(); }
            Finished = true;
            Exited?.Invoke(this);
        }
    }

    private void Process(ConsumeResult<string, string> cr)
    {
        var s = _ctx.Settings;
        lock (_lock) _positions[cr.Partition.Value] = cr.Offset.Value;

        OrderMessage? order;
        try
        {
            order = JsonSerializer.Deserialize<OrderMessage>(cr.Message.Value, Json);
            if (order is null || string.IsNullOrEmpty(order.OrderId)) throw new JsonException("пустой заказ");
        }
        catch (Exception ex)
        {
            // Poison pill: сообщение невозможно разобрать — ретраи бессмысленны, сразу в DLQ.
            Interlocked.Increment(ref _dlq);
            _ctx.SendToDlq(cr, $"poison pill: {ex.Message}", ClientId, 1);
            _ctx.Log.Add("warn",
                $"{ClientId}: poison pill в orders-{cr.Partition.Value}@{cr.Offset.Value} ({ex.Message}) → orders.dlq",
                $"{ClientId}: poison pill at orders-{cr.Partition.Value}@{cr.Offset.Value} ({ex.Message}) → orders.dlq", "dlq", "poison-" + Id, 2000);
            return;
        }

        if (!_ctx.Registry.TryMark(order.OrderId))
        {
            Interlocked.Increment(ref _duplicates);
            _ctx.Log.Add("warn",
                $"{ClientId}: заказ {order.OrderId} уже обрабатывали — повторная доставка (at-least-once). Нужна идемпотентная обработка!",
                $"{ClientId}: order {order.OrderId} was already processed — redelivery (at-least-once). Processing must be idempotent!",
                "at-least-once", "dup-" + Id, 3000);
        }

        var attempt = 0;
        while (true)
        {
            if (s.ProcessingDelayMs > 0) Thread.Sleep(s.ProcessingDelayMs);
            var failed = s.FailureRatePercent > 0 && Random.Shared.NextDouble() * 100 < s.FailureRatePercent;
            if (!failed) break;

            attempt++;
            Interlocked.Increment(ref _retries);
            if (attempt > s.MaxRetries)
            {
                Interlocked.Increment(ref _failed);
                Interlocked.Increment(ref _dlq);
                _ctx.SendToDlq(cr, "processing failed after retries (simulated)", ClientId, attempt);
                _ctx.Log.Add("warn",
                    $"{ClientId}: {order.OrderId} не обработан за {attempt} попыток → orders.dlq",
                    $"{ClientId}: {order.OrderId} failed after {attempt} attempts → orders.dlq", "dlq", "dlq-" + Id, 3000);
                Processed.Add();
                return;
            }
        }

        var payment = new PaymentMessage($"pay-{order.OrderId}", order.OrderId, order.CustomerId, order.Amount, order.Currency,
            "captured", ClientId, DateTimeOffset.UtcNow);
        _ctx.PublishPayment(payment, JsonSerializer.Serialize(payment, Json));
        Processed.Add();
    }

    private string AssignedText()
    {
        lock (_lock) return string.Join(", ", _assigned.Select(p => $"orders-{p}"));
    }

    private static string Fmt(IEnumerable<TopicPartition> parts) =>
        string.Join(", ", parts.Select(p => $"{p.Topic}-{p.Partition.Value}"));

    public object GetStats()
    {
        lock (_lock)
        {
            return new
            {
                id = Id,
                clientId = ClientId,
                state = _state,
                memberId = _memberId,
                assigned = _assigned.ToArray(),
                positions = _positions.ToDictionary(kv => kv.Key.ToString(), kv => kv.Value),
                processed = Processed.Total,
                rate = Processed.Rate,
                failed = Interlocked.Read(ref _failed),
                retries = Interlocked.Read(ref _retries),
                dlq = Interlocked.Read(ref _dlq),
                duplicates = Interlocked.Read(ref _duplicates),
                rebalances = Interlocked.Read(ref _rebalances),
                stuckForMs = _stuck ? Environment.TickCount64 - _stuckSince : 0,
                lastError = _lastError,
                strategy = _consumerSettings.AssignmentStrategy,
                staticMember = _consumerSettings.StaticMembership,
            };
        }
    }

    public long Duplicates => Interlocked.Read(ref _duplicates);
    public long Failed => Interlocked.Read(ref _failed);
    public long Retries => Interlocked.Read(ref _retries);
}
