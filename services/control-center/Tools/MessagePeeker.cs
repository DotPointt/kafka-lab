using System.Text;
using Confluent.Kafka;

namespace ControlCenter.Tools;

public sealed record PeekedMessage(
    int Partition, long Offset, long Timestamp, string? Key, string? Value, int ValueBytes, Dictionary<string, string> Headers);

/// <summary>
/// Читает последние N сообщений партиции «без группы»: Assign() на нужный offset, никаких коммитов.
/// Так работают kafka-console-consumer с --partition/--offset и UI-инструменты.
/// </summary>
public sealed class MessagePeeker : IDisposable
{
    private readonly IConsumer<byte[]?, byte[]?> _consumer;
    private readonly IProducer<string?, string?> _producer;
    private readonly SemaphoreSlim _gate = new(1, 1);

    public MessagePeeker(LabOptions options)
    {
        _consumer = new ConsumerBuilder<byte[]?, byte[]?>(new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = "control-center-peek",
            ClientId = "control-center-peek",
            EnableAutoCommit = false,
            EnablePartitionEof = true,
            AutoOffsetReset = AutoOffsetReset.Earliest,
        }).SetLogHandler((_, _) => { }).SetErrorHandler((_, _) => { }).Build();

        _producer = new ProducerBuilder<string?, string?>(new ProducerConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = "control-center",
            Acks = Acks.All,
            EnableIdempotence = true,
            MessageTimeoutMs = 15000,
            Partitioner = Partitioner.Murmur2Random,
        }).SetLogHandler((_, _) => { }).Build();
    }

    public async Task<object> PeekAsync(string topic, IReadOnlyList<int> partitions, int limit, CancellationToken ct)
    {
        await _gate.WaitAsync(ct);
        try
        {
            return await Task.Run(() => Peek(topic, partitions, Math.Clamp(limit, 1, 200)), ct);
        }
        finally
        {
            _gate.Release();
        }
    }

    private object Peek(string topic, IReadOnlyList<int> partitions, int limit)
    {
        var assignments = new List<TopicPartitionOffset>();
        var ends = new Dictionary<int, long>();
        var watermarks = new List<object>();
        var errors = new List<string>();

        foreach (var p in partitions)
        {
            try
            {
                var wm = _consumer.QueryWatermarkOffsets(new TopicPartition(topic, p), TimeSpan.FromSeconds(2));
                watermarks.Add(new { partition = p, low = wm.Low.Value, high = wm.High.Value });
                if (wm.High.Value <= wm.Low.Value) continue;
                assignments.Add(new TopicPartitionOffset(topic, p, Math.Max(wm.Low.Value, wm.High.Value - limit)));
                ends[p] = wm.High.Value;
            }
            catch (KafkaException ex)
            {
                errors.Add($"{topic}-{p}: {ex.Error.Reason}");
            }
        }

        var messages = new List<PeekedMessage>();
        if (assignments.Count > 0)
        {
            _consumer.Assign(assignments);
            var done = new HashSet<int>();
            var deadline = DateTime.UtcNow.AddSeconds(4);
            try
            {
                while (done.Count < assignments.Count && DateTime.UtcNow < deadline)
                {
                    ConsumeResult<byte[]?, byte[]?>? cr;
                    try
                    {
                        cr = _consumer.Consume(TimeSpan.FromMilliseconds(300));
                    }
                    catch (ConsumeException ex)
                    {
                        errors.Add(ex.Error.Reason);
                        continue;
                    }
                    if (cr is null) continue;
                    if (cr.IsPartitionEOF)
                    {
                        done.Add(cr.Partition.Value);
                        continue;
                    }
                    var m = cr.Message;
                    var value = m.Value is null ? null : Encoding.UTF8.GetString(m.Value);
                    if (value is { Length: > 1500 }) value = value[..1500] + "…";
                    messages.Add(new PeekedMessage(
                        cr.Partition.Value, cr.Offset.Value, m.Timestamp.UnixTimestampMs,
                        m.Key is null ? null : Encoding.UTF8.GetString(m.Key),
                        value, m.Value?.Length ?? 0,
                        m.Headers?.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes())) ?? []));
                    if (cr.Offset.Value >= ends[cr.Partition.Value] - 1) done.Add(cr.Partition.Value);
                }
            }
            finally
            {
                _consumer.Unassign();
            }
        }

        return new
        {
            topic,
            watermarks,
            errors,
            messages = messages.OrderByDescending(m => m.Timestamp).ThenByDescending(m => m.Offset).Take(limit).ToList(),
        };
    }

    public async Task<object> ProduceAsync(string topic, string? key, string? value, Dictionary<string, string>? headers)
    {
        var msg = new Message<string?, string?> { Key = key, Value = value, Headers = new Headers() };
        foreach (var (k, v) in headers ?? []) msg.Headers.Add(k, Encoding.UTF8.GetBytes(v));
        try
        {
            var dr = await _producer.ProduceAsync(topic, msg);
            return new { ok = true, partition = dr.Partition.Value, offset = dr.Offset.Value };
        }
        catch (ProduceException<string?, string?> ex)
        {
            return new { ok = false, error = ex.Error.Code.ToString(), reason = ex.Error.Reason };
        }
    }

    public void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
        _producer.Dispose();
    }
}
