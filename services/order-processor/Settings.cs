namespace OrderProcessor;

public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = "localhost:19092,localhost:19093,localhost:19094";
    public string GroupId { get; set; } = "order-processing";
    public string OrdersTopic { get; set; } = "orders";
    public string PaymentsTopic { get; set; } = "payments";
    public string DlqTopic { get; set; } = "orders.dlq";
    public int InitialInstances { get; set; } = 2;
    public int MaxInstances { get; set; } = 10;
}

public sealed record ProcessorSettings
{
    // --- «бизнес-логика», применяется на лету ---
    /// <summary>Сколько миллисекунд «обрабатывается» один заказ. Больше → ниже пропускная способность → растёт lag.</summary>
    public int ProcessingDelayMs { get; init; } = 5;
    /// <summary>Вероятность ошибки обработки, % (ошибка → ретраи → DLQ).</summary>
    public double FailureRatePercent { get; init; } = 0;
    /// <summary>Сколько раз повторять обработку перед отправкой в DLQ.</summary>
    public int MaxRetries { get; init; } = 2;

    // --- настройки consumer (при изменении все консюмеры пересоздаются) ---
    /// <summary>range | roundrobin | cooperative-sticky | consumer (новый протокол KIP-848, назначение считает брокер)</summary>
    public string AssignmentStrategy { get; init; } = "cooperative-sticky";
    /// <summary>Без heartbeat дольше этого времени координатор считает консюмер мёртвым.</summary>
    public int SessionTimeoutMs { get; init; } = 10000;
    /// <summary>Если приложение не вызывает Consume() дольше — консюмер сам выходит из группы.</summary>
    public int MaxPollIntervalMs { get; init; } = 20000;
    /// <summary>Как часто librdkafka коммитит сохранённые (StoreOffset) offset-ы. Это окно возможных дублей после крэша.</summary>
    public int AutoCommitIntervalMs { get; init; } = 5000;
    /// <summary>Static membership (group.instance.id): перезапуск в пределах session.timeout не вызывает ребаланс.</summary>
    public bool StaticMembership { get; init; } = false;

    public bool SameConsumerConfig(ProcessorSettings o) =>
        AssignmentStrategy == o.AssignmentStrategy && SessionTimeoutMs == o.SessionTimeoutMs &&
        MaxPollIntervalMs == o.MaxPollIntervalMs && AutoCommitIntervalMs == o.AutoCommitIntervalMs &&
        StaticMembership == o.StaticMembership;
}

public sealed record SettingsPatch(
    int? ProcessingDelayMs, double? FailureRatePercent, int? MaxRetries,
    string? AssignmentStrategy, int? SessionTimeoutMs, int? MaxPollIntervalMs,
    int? AutoCommitIntervalMs, bool? StaticMembership);

public sealed record OrderMessage(string OrderId, string CustomerId, decimal Amount, string Currency, int Items, DateTimeOffset CreatedAt);

public sealed record PaymentMessage(string PaymentId, string OrderId, string CustomerId, decimal Amount, string Currency,
    string Status, string ProcessedBy, DateTimeOffset ProcessedAt);
