namespace OrderService;

public sealed class KafkaOptions
{
    /// <summary>Внутри docker-сети — kafka-1:9092,...; при запуске из IDE на хосте — localhost:19092,...</summary>
    public string BootstrapServers { get; set; } = "localhost:19092,localhost:19093,localhost:19094";
    public string OrdersTopic { get; set; } = "orders";
    public string ProfilesTopic { get; set; } = "customer-profiles";
}

/// <summary>Текущие настройки генератора и producer. Меняются на лету через PUT /api/config.</summary>
public sealed record ProducerSettings
{
    // --- генератор нагрузки (применяются мгновенно) ---
    /// <summary>Сколько заказов в секунду генерировать.</summary>
    public double RatePerSec { get; init; } = 20;
    /// <summary>Доля заказов от одного «горячего» клиента (customer-007) — показывает перекос партиций.</summary>
    public int HotKeyPercent { get; init; } = 0;
    /// <summary>Сколько разных клиентов (ключей).</summary>
    public int Customers { get; init; } = 50;
    /// <summary>Публиковать ли профиль клиента в compacted-топик customer-profiles.</summary>
    public bool PublishProfiles { get; init; } = true;

    // --- настройки Kafka producer (при изменении producer пересоздаётся) ---
    /// <summary>acks: "0" — не ждать ответа, "1" — ждать только лидера, "all" — ждать все реплики из ISR.</summary>
    public string Acks { get; init; } = "all";
    /// <summary>Идемпотентный producer: брокер отбрасывает дубли ретраев по (ProducerId, sequence).</summary>
    public bool EnableIdempotence { get; init; } = true;
    /// <summary>Сколько ждать, набирая batch, прежде чем отправить (linger.ms).</summary>
    public int LingerMs { get; init; } = 5;
    /// <summary>none | gzip | snappy | lz4 | zstd</summary>
    public string Compression { get; init; } = "none";
    /// <summary>Таймаут одного запроса к брокеру (socket.timeout.ms + request.timeout.ms в librdkafka).</summary>
    public int RequestTimeoutMs { get; init; } = 10000;
    /// <summary>Сколько всего producer пытается доставить сообщение, включая ретраи (delivery.timeout.ms / message.timeout.ms).</summary>
    public int DeliveryTimeoutMs { get; init; } = 30000;
    /// <summary>max.in.flight.requests.per.connection</summary>
    public int MaxInFlight { get; init; } = 5;

    public bool SameProducerConfig(ProducerSettings o) =>
        Acks == o.Acks && EnableIdempotence == o.EnableIdempotence && LingerMs == o.LingerMs &&
        Compression == o.Compression && RequestTimeoutMs == o.RequestTimeoutMs &&
        DeliveryTimeoutMs == o.DeliveryTimeoutMs && MaxInFlight == o.MaxInFlight;
}

/// <summary>Частичное обновление настроек: null = не менять.</summary>
public sealed record SettingsPatch(
    double? RatePerSec, int? HotKeyPercent, int? Customers, bool? PublishProfiles,
    string? Acks, bool? EnableIdempotence, int? LingerMs, string? Compression,
    int? RequestTimeoutMs, int? DeliveryTimeoutMs, int? MaxInFlight);

public sealed record ManualOrderRequest(string? CustomerId, decimal? Amount);

public sealed record BurstRequest(int Count);

public sealed record Order(
    string OrderId, string CustomerId, decimal Amount, string Currency,
    int Items, DateTimeOffset CreatedAt, string Source);

public sealed record CustomerProfile(
    string CustomerId, int OrdersCount, decimal TotalSpent, string Tier, DateTimeOffset UpdatedAt);
