using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using OrderService.Infrastructure;

namespace OrderService;

/// <summary>
/// Обёртка над IProducer: создаёт producer по текущим настройкам, отправляет заказы
/// и собирает статистику (скорость, задержка подтверждения, ошибки, распределение по партициям).
/// </summary>
public sealed class OrderProducer : IDisposable
{
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);
    private static readonly string[] Currencies = ["USD", "EUR", "RUB"];

    private readonly KafkaOptions _kafka;
    private readonly LabLog _log;
    private readonly DeliveryVerifier _verifier;
    private readonly Lock _gate = new();
    private readonly long _startedAt = Environment.TickCount64;

    private IProducer<string, string?> _producer;
    private ProducerSettings _settings = new();

    // --- статистика ---
    public RateCounter Sent { get; } = new();
    public RateCounter Acked { get; } = new();
    public RateCounter Failed { get; } = new();
    public RateCounter ProfilesSent { get; } = new();
    private readonly LatencyTracker _latency = new();
    private readonly long[] _partitionAcks = new long[64];
    private readonly ConcurrentDictionary<string, long> _errorCounts = new();
    private readonly ConcurrentQueue<object> _recentErrors = new();
    private readonly ConcurrentQueue<object> _recent = new();
    private readonly ConcurrentDictionary<string, CustomerProfile> _profiles = new();
    // Метка запуска в orderId: после рестарта сервиса номера не повторяются (иначе консюмер принял бы их за дубли)
    private readonly string _run = $"{(char)('a' + Random.Shared.Next(26))}{(char)('a' + Random.Shared.Next(26))}";
    private long _seq, _queuedMsgs, _queueFull, _possiblyPersisted, _notPersisted, _producerGeneration;
    private volatile object? _clientView;

    public OrderProducer(KafkaOptions kafka, LabLog log, DeliveryVerifier verifier)
    {
        _kafka = kafka;
        _log = log;
        _verifier = verifier;
        _producer = Build(_settings);
    }

    public ProducerSettings Settings => _settings;

    // ------------------------------------------------------------------ producer

    private IProducer<string, string?> Build(ProducerSettings s)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _kafka.BootstrapServers,
            ClientId = "order-service",
            Acks = s.Acks switch { "0" => Confluent.Kafka.Acks.None, "1" => Confluent.Kafka.Acks.Leader, _ => Confluent.Kafka.Acks.All },
            EnableIdempotence = s.EnableIdempotence,
            LingerMs = s.LingerMs,
            CompressionType = Enum.Parse<CompressionType>(s.Compression, ignoreCase: true),
            // В librdkafka таймаут ожидания ответа на ProduceRequest на стороне клиента — socket.timeout.ms,
            // а request.timeout.ms — сколько брокер ждёт реплики при acks=all.
            SocketTimeoutMs = s.RequestTimeoutMs,
            RequestTimeoutMs = s.RequestTimeoutMs,
            MessageTimeoutMs = s.DeliveryTimeoutMs,
            MaxInFlight = s.MaxInFlight,
            RetryBackoffMs = 200,
            RetryBackoffMaxMs = 1000,
            // ВАЖНО: по умолчанию librdkafka использует consistent_random (CRC32), а Java-клиент — murmur2.
            // Чтобы ключ попадал в ту же партицию, что и у Java/Go-клиентов, явно включаем murmur2.
            Partitioner = Confluent.Kafka.Partitioner.Murmur2Random,
            StatisticsIntervalMs = 1000,
            TopicMetadataRefreshIntervalMs = 30000,
            ReconnectBackoffMs = 200,
            ReconnectBackoffMaxMs = 2000,
        };

        return new ProducerBuilder<string, string?>(config)
            .SetStatisticsHandler((_, json) => OnStatistics(json))
            .SetErrorHandler((p, e) =>
            {
                _log.Add(e.IsFatal ? "error" : "warn", $"librdkafka: {e.Reason}", $"librdkafka: {e.Reason}", LearnFor(e.Code), "client-error:" + e.Code, 8000);
                if (e.IsFatal) OnFatal(p, e);
            })
            .SetLogHandler((_, m) =>
            {
                if (m.Level <= SyslogLevel.Warning)
                    _log.Add("warn", $"librdkafka [{m.Facility}]: {m.Message}", $"librdkafka [{m.Facility}]: {m.Message}", null, "log:" + m.Facility, 8000);
            })
            .Build();
    }

    public object ApplySettings(SettingsPatch p)
    {
        var notes = new List<string>();
        var cur = _settings;
        var next = cur with
        {
            RatePerSec = Math.Clamp(p.RatePerSec ?? cur.RatePerSec, 0, 5000),
            HotKeyPercent = Math.Clamp(p.HotKeyPercent ?? cur.HotKeyPercent, 0, 100),
            Customers = Math.Clamp(p.Customers ?? cur.Customers, 1, 1000),
            PublishProfiles = p.PublishProfiles ?? cur.PublishProfiles,
            Acks = p.Acks is "0" or "1" or "all" ? p.Acks : cur.Acks,
            EnableIdempotence = p.EnableIdempotence ?? cur.EnableIdempotence,
            LingerMs = Math.Clamp(p.LingerMs ?? cur.LingerMs, 0, 2000),
            Compression = p.Compression is "none" or "gzip" or "snappy" or "lz4" or "zstd" ? p.Compression : cur.Compression,
            RequestTimeoutMs = Math.Clamp(p.RequestTimeoutMs ?? cur.RequestTimeoutMs, 100, 120000),
            DeliveryTimeoutMs = Math.Clamp(p.DeliveryTimeoutMs ?? cur.DeliveryTimeoutMs, 1000, 600000),
            MaxInFlight = Math.Clamp(p.MaxInFlight ?? cur.MaxInFlight, 1, 50),
        };

        // Правила совместимости — те же, что проверяет сам клиент.
        if (next.EnableIdempotence && next.Acks != "all")
        {
            if (p.EnableIdempotence == true) next = next with { Acks = "all" };
            else next = next with { EnableIdempotence = false };
            notes.Add("Идемпотентность требует acks=all");
        }
        if (next.EnableIdempotence && next.MaxInFlight > 5)
        {
            next = next with { MaxInFlight = 5 };
            notes.Add("Идемпотентность допускает max.in.flight ≤ 5");
        }
        if (next.DeliveryTimeoutMs <= next.LingerMs)
        {
            next = next with { DeliveryTimeoutMs = next.LingerMs + next.RequestTimeoutMs };
            notes.Add("delivery.timeout.ms должен быть больше linger.ms");
        }

        var recreate = !next.SameProducerConfig(cur);
        if (recreate)
        {
            Recreate(next);
            var cfg = $"acks={next.Acks}, idempotence={(next.EnableIdempotence ? "on" : "off")}, linger={next.LingerMs}ms, " +
                      $"compression={next.Compression}, request.timeout={next.RequestTimeoutMs}ms, delivery.timeout={next.DeliveryTimeoutMs}ms";
            _log.Add("info",
                $"Producer пересоздан: {cfg}. Сообщения, оставшиеся в буфере старого producer-а, будут выброшены (Local_PurgeQueue), если не успеют уйти",
                $"Producer recreated: {cfg}. Messages still buffered in the old producer are dropped (Local_PurgeQueue) if they can't be sent in time",
                "producer-config");
        }
        else
        {
            _settings = next;
        }

        return new { settings = next, recreated = recreate, notes };
    }

    private void Recreate(ProducerSettings next, bool flushOld = true)
    {
        IProducer<string, string?> old;
        var fresh = Build(next);
        lock (_gate)
        {
            old = _producer;
            _producer = fresh;
            _settings = next;
            Interlocked.Increment(ref _producerGeneration);
        }
        // Старый producer дожидается подтверждений уже отправленных сообщений и закрывается в фоне.
        _ = Task.Run(() =>
        {
            if (flushOld)
                try { old.Flush(TimeSpan.FromSeconds(Math.Min(10, next.DeliveryTimeoutMs / 1000.0))); } catch { }
            old.Dispose();
        });
    }

    private long _lastFatalAt;

    /// <summary>
    /// Фатальная ошибка (обычно у идемпотентного producer-а, когда librdkafka больше не может гарантировать
    /// порядок/отсутствие дублей после череды таймаутов). Такой экземпляр навсегда перестаёт отправлять —
    /// единственный выход: создать новый producer (он получит новый ProducerId).
    /// </summary>
    private void OnFatal(IProducer<string, string?> failed, Error e)
    {
        lock (_gate)
            if (!ReferenceEquals(failed, _producer)) return; // уже заменён
        var now = Environment.TickCount64;
        if (now - Interlocked.Read(ref _lastFatalAt) < 5000) return;
        Interlocked.Exchange(ref _lastFatalAt, now);
        _log.Add("error",
            $"Фатальная ошибка producer-а: {e.Reason}. Этот экземпляр больше не может отправлять — создаём новый (новый ProducerId). Неотправленные сообщения старого будут потеряны для приложения",
            $"Fatal producer error: {e.Reason}. This instance can't send anymore — creating a new one (new ProducerId). Unsent messages of the old one are lost to the application",
            "fatal-producer");
        _ = Task.Run(() => Recreate(_settings, flushOld: false));
    }

    // ------------------------------------------------------------------ отправка

    /// <summary>Сгенерировать и асинхронно отправить один заказ. false — локальная очередь переполнена.</summary>
    public bool ProduceGenerated()
    {
        var s = _settings;
        var order = NewOrder(PickCustomer(s), null);
        return Produce(order, s);
    }

    private bool Produce(Order order, ProducerSettings s)
    {
        var message = BuildMessage(order);
        var started = Stopwatch.GetTimestamp();
        IProducer<string, string?> producer;
        lock (_gate) producer = _producer;
        try
        {
            // Produce() не блокирует: сообщение кладётся в локальный буфер (RecordAccumulator),
            // фоновый поток librdkafka собирает batch-и и шлёт их лидерам партиций.
            producer.Produce(_kafka.OrdersTopic, message, dr => OnDelivery(dr, order, started));
            Sent.Add();
        }
        catch (ProduceException<string, string?> ex) when (ex.Error.Code == ErrorCode.Local_QueueFull)
        {
            Interlocked.Increment(ref _queueFull);
            _log.Add("warn",
                "Локальный буфер producer переполнен (Local_QueueFull): брокеры не успевают подтверждать — генератор притормаживает (backpressure)",
                "The producer's local buffer is full (Local_QueueFull): brokers can't acknowledge fast enough — the generator slows down (backpressure)",
                "backpressure", "queue-full", 10000);
            return false;
        }
        catch (ObjectDisposedException) { return false; }
        catch (KafkaException ex)
        {
            Failed.Add();
            RecordError(ex.Error.Code, ex.Error.Reason, null);
            return false;
        }

        if (s.PublishProfiles) PublishProfile(producer, order);
        return true;
    }

    private void OnDelivery(DeliveryReport<string, string?> dr, Order order, long started)
    {
        if (dr.Error.IsError)
        {
            Failed.Add();
            if (dr.Status == PersistenceStatus.PossiblyPersisted) Interlocked.Increment(ref _possiblyPersisted);
            else Interlocked.Increment(ref _notPersisted);
            RecordError(dr.Error.Code, dr.Error.Reason, dr.Status);
            return;
        }

        var latency = Stopwatch.GetElapsedTime(started).TotalMilliseconds;
        Acked.Add();
        _latency.Record(latency);
        var partition = dr.Partition.Value;
        if (partition is >= 0 and < 64) Interlocked.Increment(ref _partitionAcks[partition]);
        _verifier.OnAcked(partition, dr.Offset.Value, order.OrderId);

        _recent.Enqueue(new
        {
            orderId = order.OrderId,
            key = order.CustomerId,
            partition,
            offset = dr.Offset.Value,
            latencyMs = Math.Round(latency, 1),
            ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        });
        while (_recent.Count > 15) _recent.TryDequeue(out _);
    }

    /// <summary>Ручная отправка одного заказа с ожиданием подтверждения — чтобы увидеть партицию и offset.</summary>
    public async Task<object> SendManualAsync(ManualOrderRequest req)
    {
        var s = _settings;
        var customer = string.IsNullOrWhiteSpace(req.CustomerId) ? PickCustomer(s) : req.CustomerId.Trim();
        var order = NewOrder(customer, req.Amount);
        IProducer<string, string?> producer;
        lock (_gate) producer = _producer;
        var sw = Stopwatch.StartNew();
        try
        {
            Sent.Add();
            var dr = await producer.ProduceAsync(_kafka.OrdersTopic, BuildMessage(order));
            Acked.Add();
            _verifier.OnAcked(dr.Partition.Value, dr.Offset.Value, order.OrderId);
            if (s.PublishProfiles) PublishProfile(producer, order);
            _log.Add("info",
                $"Ручной заказ {order.OrderId}: ключ «{customer}» → партиция {dr.Partition.Value}, offset {dr.Offset.Value}",
                $"Manual order {order.OrderId}: key \"{customer}\" → partition {dr.Partition.Value}, offset {dr.Offset.Value}", "key-partition");
            return new
            {
                ok = true, orderId = order.OrderId, key = customer, partition = dr.Partition.Value,
                offset = dr.Offset.Value, latencyMs = Math.Round(sw.Elapsed.TotalMilliseconds, 1),
                status = dr.Status.ToString(), acks = s.Acks,
            };
        }
        catch (ProduceException<string, string?> ex)
        {
            Failed.Add();
            RecordError(ex.Error.Code, ex.Error.Reason, ex.DeliveryResult?.Status);
            return new { ok = false, orderId = order.OrderId, key = customer, error = ex.Error.Code.ToString(), reason = ex.Error.Reason, status = ex.DeliveryResult?.Status.ToString() };
        }
    }

    /// <summary>Отправить пачку из N заказов максимально быстро (в фоне).</summary>
    public object Burst(int count)
    {
        count = Math.Clamp(count, 1, 1_000_000);
        _log.Add("info", $"Burst: отправляем {count} заказов так быстро, как сможем", $"Burst: sending {count} orders as fast as possible", "batching");
        _ = Task.Run(() =>
        {
            var sent = 0;
            var sw = Stopwatch.StartNew();
            while (sent < count && sw.Elapsed < TimeSpan.FromMinutes(5))
            {
                if (ProduceGenerated()) sent++;
                else Thread.Sleep(5);
            }
            _log.Add("info", $"Burst завершён: {sent} заказов поставлено в очередь за {sw.Elapsed.TotalSeconds:F1} c",
                $"Burst finished: {sent} orders queued in {sw.Elapsed.TotalSeconds:F1} s", "batching");
        });
        return new { accepted = count };
    }

    /// <summary>Tombstone (value = null): в compacted-топике удаляет ключ при следующей чистке.</summary>
    public async Task<object> DeleteProfileAsync(string customerId)
    {
        IProducer<string, string?> producer;
        lock (_gate) producer = _producer;
        _profiles.TryRemove(customerId, out _);
        var dr = await producer.ProduceAsync(_kafka.ProfilesTopic, new Message<string, string?> { Key = customerId, Value = null });
        _log.Add("info",
            $"Tombstone для {customerId} → customer-profiles[{dr.Partition.Value}]@{dr.Offset.Value}: после компакции ключ исчезнет",
            $"Tombstone for {customerId} → customer-profiles[{dr.Partition.Value}]@{dr.Offset.Value}: the key disappears after compaction", "compaction");
        return new { ok = true, partition = dr.Partition.Value, offset = dr.Offset.Value };
    }

    private void PublishProfile(IProducer<string, string?> producer, Order order)
    {
        var profile = _profiles.AddOrUpdate(order.CustomerId,
            _ => new CustomerProfile(order.CustomerId, 1, order.Amount, Tier(order.Amount), DateTimeOffset.UtcNow),
            (_, p) => p with
            {
                OrdersCount = p.OrdersCount + 1,
                TotalSpent = p.TotalSpent + order.Amount,
                Tier = Tier(p.TotalSpent + order.Amount),
                UpdatedAt = DateTimeOffset.UtcNow,
            });
        try
        {
            producer.Produce(_kafka.ProfilesTopic,
                new Message<string, string?> { Key = profile.CustomerId, Value = JsonSerializer.Serialize(profile, Json) },
                dr => { if (!dr.Error.IsError) ProfilesSent.Add(); });
        }
        catch (KafkaException) { /* профили — второстепенный поток, ошибки не важны */ }
        catch (ObjectDisposedException) { }

        static string Tier(decimal total) => total switch { > 50_000 => "platinum", > 10_000 => "gold", > 2_000 => "silver", _ => "bronze" };
    }

    // ------------------------------------------------------------------ helpers

    private Order NewOrder(string customerId, decimal? amount)
    {
        var seq = Interlocked.Increment(ref _seq);
        var rnd = Random.Shared;
        return new Order(
            OrderId: $"o-{_run}{seq}",
            CustomerId: customerId,
            Amount: amount ?? Math.Round((decimal)(rnd.NextDouble() * 490 + 10), 2),
            Currency: Currencies[rnd.Next(Currencies.Length)],
            Items: rnd.Next(1, 6),
            CreatedAt: DateTimeOffset.UtcNow,
            Source: "order-service");
    }

    private static string PickCustomer(ProducerSettings s)
    {
        var rnd = Random.Shared;
        if (s.HotKeyPercent > 0 && rnd.Next(100) < s.HotKeyPercent) return "customer-007";
        return $"customer-{rnd.Next(1, s.Customers + 1):D3}";
    }

    private static Message<string, string?> BuildMessage(Order order) => new()
    {
        // Ключ определяет партицию: hash(key) % partitions. Все заказы клиента попадут в одну партицию,
        // а значит обработаются строго по порядку.
        Key = order.CustomerId,
        Value = JsonSerializer.Serialize(order, Json),
        Headers = new Headers
        {
            { "source", "order-service"u8.ToArray() },
            { "order-id", Encoding.UTF8.GetBytes(order.OrderId) },
            { "content-type", "application/json"u8.ToArray() },
        },
    };

    private void RecordError(ErrorCode code, string reason, PersistenceStatus? status)
    {
        var name = code.ToString();
        _errorCounts.AddOrUpdate(name, 1, (_, v) => v + 1);
        _recentErrors.Enqueue(new { ts = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), code = name, reason, status = status?.ToString() });
        while (_recentErrors.Count > 10) _recentErrors.TryDequeue(out _);
        _log.Add("error",
            $"Доставка не удалась: {name} — {reason}" + (status is null ? "" : $" (статус: {status})"),
            $"Delivery failed: {name} — {reason}" + (status is null ? "" : $" (status: {status})"),
            LearnFor(code), "delivery-error:" + name, 5000);
    }

    private static string? LearnFor(ErrorCode code) => code switch
    {
        ErrorCode.Local_MsgTimedOut => "delivery-timeout",
        ErrorCode.NotEnoughReplicas or ErrorCode.NotEnoughReplicasAfterAppend => "min-isr",
        ErrorCode.Local_QueueFull => "backpressure",
        ErrorCode.NotLeaderForPartition or ErrorCode.LeaderNotAvailable => "leader-election",
        ErrorCode.RequestTimedOut or ErrorCode.Local_TimedOut => "request-timeout",
        ErrorCode.Local_AllBrokersDown or ErrorCode.Local_Transport => "broker-down",
        ErrorCode.Local_PurgeQueue or ErrorCode.Local_PurgeInflight => "producer-config",
        ErrorCode.Local_Inconsistent or ErrorCode.Local_Fatal => "fatal-producer",
        _ => null,
    };

    /// <summary>librdkafka раз в секунду присылает JSON со своей внутренней статистикой — берём «взгляд клиента» на брокеры.</summary>
    private void OnStatistics(string json)
    {
        try
        {
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            var brokers = new List<ClientBrokerView>();
            foreach (var b in root.GetProperty("brokers").EnumerateObject())
            {
                var v = b.Value;
                var id = v.GetProperty("nodeid").GetInt32();
                if (id < 0) continue; // bootstrap-соединения
                brokers.Add(new ClientBrokerView(
                    id,
                    v.GetProperty("state").GetString(),
                    Math.Round(v.GetProperty("rtt").GetProperty("avg").GetInt64() / 1000.0, 1),
                    Math.Round(v.GetProperty("rtt").GetProperty("p99").GetInt64() / 1000.0, 1),
                    v.GetProperty("outbuf_msg_cnt").GetInt64(),
                    v.GetProperty("waitresp_msg_cnt").GetInt64(),
                    v.GetProperty("req_timeouts").GetInt64()));
            }

            // Как клиент видит лидеров партиций orders (может отличаться от кластера, пока не обновил метаданные!)
            var leaders = new Dictionary<string, int>();
            if (root.TryGetProperty("topics", out var topics) && topics.TryGetProperty(_kafka.OrdersTopic, out var t))
            {
                foreach (var p in t.GetProperty("partitions").EnumerateObject())
                {
                    if (p.Name == "-1") continue;
                    leaders[p.Name] = p.Value.GetProperty("leader").GetInt32();
                }
            }

            // msg_cnt — сколько сообщений producer ещё не довёл до delivery report (буфер + в полёте)
            Interlocked.Exchange(ref _queuedMsgs, root.GetProperty("msg_cnt").GetInt64());
            _clientView = new
            {
                queuedMsgs = root.GetProperty("msg_cnt").GetInt64(),
                queuedBytes = root.GetProperty("msg_size").GetInt64(),
                brokers = brokers.OrderBy(x => x.Id).ToList(),
                leaders,
            };
        }
        catch (Exception)
        {
            // статистика — best effort
        }
    }

    public object GetStats() => new
    {
        service = "order-service",
        lang = "C#",
        role = "producer",
        instanceId = Environment.MachineName,
        uptimeSec = (Environment.TickCount64 - _startedAt) / 1000,
        topic = _kafka.OrdersTopic,
        config = _settings,
        producer = new
        {
            sent = Sent.Total,
            acked = Acked.Total,
            failed = Failed.Total,
            inFlight = Interlocked.Read(ref _queuedMsgs),
            queueFull = Interlocked.Read(ref _queueFull),
            possiblyPersisted = Interlocked.Read(ref _possiblyPersisted),
            notPersisted = Interlocked.Read(ref _notPersisted),
            generation = Interlocked.Read(ref _producerGeneration),
            rates = new { sent = Sent.Rate, acked = Acked.Rate, failed = Failed.Rate, profiles = ProfilesSent.Rate },
            latencyMs = _latency.Snapshot(),
            partitions = _partitionAcks.Select((v, i) => (v, i)).Where(x => x.v > 0).ToDictionary(x => x.i.ToString(), x => x.v),
            errorCounts = _errorCounts,
            recentErrors = _recentErrors.ToArray(),
        },
        profiles = new { sent = ProfilesSent.Total, rate = ProfilesSent.Rate, customers = _profiles.Count },
        client = _clientView,
        verifier = _verifier.GetStats(),
        recent = _recent.ToArray(),
        events = _log.Recent(),
    };

    private sealed record ClientBrokerView(
        int Id, string? State, double RttAvgMs, double RttP99Ms, long OutbufMsgs, long WaitRespMsgs, long ReqTimeouts);

    public void Dispose()
    {
        lock (_gate)
        {
            try { _producer.Flush(TimeSpan.FromSeconds(5)); } catch { }
            _producer.Dispose();
        }
    }
}
