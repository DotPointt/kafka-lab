using System.Text;
using Confluent.Kafka;
using OrderProcessor.Infrastructure;

namespace OrderProcessor;

/// <summary>Общие для всех консюмеров процесса ресурсы: настройки, producer для payments/DLQ, реестр обработанных заказов.</summary>
public sealed class ProcessorContext : IDisposable
{
    public KafkaOptions Kafka { get; }
    public LabLog Log { get; }
    public ProcessedRegistry Registry { get; } = new();
    public volatile ProcessorSettings Settings = new();

    public RateCounter PaymentsSent { get; } = new();
    public RateCounter PaymentsFailed { get; } = new();
    public RateCounter DlqSent { get; } = new();

    // Один producer на процесс: IProducer потокобезопасен и сам батчит сообщения от всех потоков.
    private readonly IProducer<string, string> _producer;

    public ProcessorContext(KafkaOptions kafka, LabLog log)
    {
        Kafka = kafka;
        Log = log;
        _producer = new ProducerBuilder<string, string>(new ProducerConfig
        {
            BootstrapServers = kafka.BootstrapServers,
            ClientId = "order-processor-producer",
            Acks = Acks.All,
            EnableIdempotence = true,
            LingerMs = 5,
            MessageTimeoutMs = 30000,
            Partitioner = Partitioner.Murmur2Random,
        })
        .SetLogHandler((_, _) => { })
        .Build();
    }

    public void PublishPayment(PaymentMessage payment, string json)
    {
        try
        {
            _producer.Produce(Kafka.PaymentsTopic, new Message<string, string> { Key = payment.CustomerId, Value = json },
                dr =>
                {
                    if (dr.Error.IsError)
                    {
                        PaymentsFailed.Add();
                        Log.Add("error", $"Не удалось записать платёж в payments: {dr.Error.Reason}", "delivery-timeout", "payment-fail");
                    }
                    else PaymentsSent.Add();
                });
        }
        catch (KafkaException ex)
        {
            PaymentsFailed.Add();
            Log.Add("error", $"payments: {ex.Error.Reason}", null, "payment-fail");
        }
    }

    /// <summary>Dead Letter Queue: исходное сообщение + заголовки с причиной и координатами оригинала.</summary>
    public void SendToDlq(ConsumeResult<string, string> cr, string reason, string failedBy, int attempts)
    {
        var headers = new Headers
        {
            { "dlq-reason", Encoding.UTF8.GetBytes(reason) },
            { "dlq-original-topic", Encoding.UTF8.GetBytes(cr.Topic) },
            { "dlq-original-partition", Encoding.UTF8.GetBytes(cr.Partition.Value.ToString()) },
            { "dlq-original-offset", Encoding.UTF8.GetBytes(cr.Offset.Value.ToString()) },
            { "dlq-failed-by", Encoding.UTF8.GetBytes(failedBy) },
            { "dlq-attempts", Encoding.UTF8.GetBytes(attempts.ToString()) },
        };
        try
        {
            _producer.Produce(Kafka.DlqTopic, new Message<string, string> { Key = cr.Message.Key, Value = cr.Message.Value, Headers = headers },
                dr => { if (!dr.Error.IsError) DlqSent.Add(); });
        }
        catch (KafkaException ex)
        {
            Log.Add("error", $"DLQ недоступна: {ex.Error.Reason}", null, "dlq-fail");
        }
    }

    public void Dispose()
    {
        try { _producer.Flush(TimeSpan.FromSeconds(5)); } catch { }
        _producer.Dispose();
    }
}

/// <summary>
/// Помнит последние ~200 тыс. обработанных orderId. Повторная обработка = дубликат:
/// так выглядит семантика at-least-once после крэша или ребаланса до коммита offset-а.
/// В реальной системе тут была бы идемпотентная запись в БД (upsert по orderId) или таблица inbox.
/// </summary>
public sealed class ProcessedRegistry
{
    private const int Capacity = 200_000;
    private readonly HashSet<string> _set = new();
    private readonly Queue<string> _order = new();
    private readonly Lock _lock = new();

    /// <returns>true — первый раз; false — уже обрабатывали (дубликат).</returns>
    public bool TryMark(string orderId)
    {
        lock (_lock)
        {
            if (!_set.Add(orderId)) return false;
            _order.Enqueue(orderId);
            if (_order.Count > Capacity) _set.Remove(_order.Dequeue());
            return true;
        }
    }
}
