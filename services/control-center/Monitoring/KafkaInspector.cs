using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace ControlCenter.Monitoring;

public sealed record PartitionMeta(int Id, int Leader, int[] Replicas, int[] Isr, string? Error);
public sealed record TopicMeta(string Name, List<PartitionMeta> Partitions, string? Error);
public sealed record MetadataResult(List<int> Brokers, List<TopicMeta> Topics);

public sealed record GroupResult(
    string Id, string State, string? Assignor, string? Type, int? Coordinator,
    List<MemberView> Members, Dictionary<(string Topic, int Partition), long>? Committed, string? Error);

/// <summary>
/// Всё, что control-center знает о кластере, он получает через обычный Kafka AdminClient (Confluent.Kafka):
///   • GetMetadata          — брокеры, топики, лидеры, реплики, ISR;
///   • ListOffsets          — начало и конец лога каждой партиции;
///   • ListConsumerGroups / DescribeConsumerGroups / ListConsumerGroupOffsets — группы, участники, назначения, коммиты.
/// Так же устроены kafka-ui, Conduktor, Burrow и другие инструменты мониторинга.
/// </summary>
public sealed class KafkaInspector : IDisposable
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(3);
    private static readonly string[] InterestingConfigs =
        ["min.insync.replicas", "retention.ms", "retention.bytes", "cleanup.policy", "segment.bytes", "unclean.leader.election.enable"];

    private readonly IAdminClient _admin;
    private readonly IConsumer<Ignore, Ignore> _offsetsConsumer;
    private readonly IAdminClient _offsetsAdmin;
    private Dictionary<string, Dictionary<string, string>> _topicConfigs = new();
    private long _configsAt;
    private string? _clusterId;
    private readonly HashSet<string> _knownGroups = ["order-processing", "analytics"];
    private long _clusterIdAt;

    public KafkaInspector(LabOptions options)
    {
        _admin = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = options.BootstrapServers,
            ClientId = "control-center",
            SocketTimeoutMs = 5000,
            ReconnectBackoffMs = 200,
            ReconnectBackoffMaxMs = 2000,
        })
        .SetLogHandler((_, _) => { })
        .SetErrorHandler((_, _) => { })
        .Build();

        // ⚠ Обход бага librdkafka (≤ 2.15): ListConsumerGroupOffsets через «чистый» AdminClient (без group.id)
        // падает с SIGSEGV/double free, когда брокер-координатор останавливается и отвечает NOT_COORDINATOR
        // (обработчик OffsetFetch обращается к несуществующему rk_cgrp). Поэтому этот запрос выполняем
        // через admin-обёртку над handle консюмера с group.id — у него rk_cgrp есть. Консюмер ни на что не подписан.
        // См. https://github.com/confluentinc/librdkafka/pull/5611
        _offsetsConsumer = new ConsumerBuilder<Ignore, Ignore>(new ConsumerConfig
        {
            BootstrapServers = options.BootstrapServers,
            GroupId = "control-center-offsets-reader",
            ClientId = "control-center-offsets",
            EnableAutoCommit = false,
            SocketTimeoutMs = 5000,
        })
        .SetLogHandler((_, _) => { })
        .SetErrorHandler((_, _) => { })
        .Build();
        _offsetsAdmin = new DependentAdminClientBuilder(_offsetsConsumer.Handle).Build();
    }

    public IAdminClient Admin => _admin;

    public static bool IsInternal(string topic) => topic.StartsWith("__", StringComparison.Ordinal);

    public MetadataResult GetMetadata()
    {
        var md = _admin.GetMetadata(Timeout);
        var topics = md.Topics
            .Where(t => !t.Topic.StartsWith("__share", StringComparison.Ordinal) && t.Topic != "__transaction_state")
            .Select(t => new TopicMeta(
                t.Topic,
                t.Partitions.Select(p => new PartitionMeta(p.PartitionId, p.Leader, p.Replicas, p.InSyncReplicas,
                    p.Error.IsError ? p.Error.Code.ToString() : null)).OrderBy(p => p.Id).ToList(),
                t.Error.IsError ? t.Error.Code.ToString() : null))
            .OrderBy(t => IsInternal(t.Name)).ThenBy(t => t.Name)
            .ToList();
        return new MetadataResult(md.Brokers.Select(b => b.BrokerId).OrderBy(x => x).ToList(), topics);
    }

    public async Task<string?> ClusterIdAsync()
    {
        if (_clusterId is not null && Environment.TickCount64 - _clusterIdAt < 60_000) return _clusterId;
        try
        {
            var r = await _admin.DescribeClusterAsync(new DescribeClusterOptions { RequestTimeout = Timeout });
            _clusterId = r.ClusterId;
            _clusterIdAt = Environment.TickCount64;
        }
        catch
        {
            // не критично
        }
        return _clusterId;
    }

    /// <summary>Latest = high watermark (до него консюмеры могут читать), Earliest = log start offset.</summary>
    public async Task<Dictionary<(string Topic, int Partition), long>> ListOffsetsAsync(
        IEnumerable<(string Topic, int Partition)> partitions, OffsetSpec spec)
    {
        var specs = partitions
            .Select(p => new TopicPartitionOffsetSpec { TopicPartition = new TopicPartition(p.Topic, p.Partition), OffsetSpec = spec })
            .ToList();
        var result = new Dictionary<(string, int), long>();
        if (specs.Count == 0) return result;

        List<ListOffsetsResultInfo> infos;
        try
        {
            infos = (await _admin.ListOffsetsAsync(specs, new ListOffsetsOptions { RequestTimeout = Timeout })).ResultInfos;
        }
        catch (ListOffsetsException e)
        {
            infos = e.Result.ResultInfos; // часть партиций может быть без лидера — берём что есть
        }

        foreach (var info in infos)
        {
            var tpo = info.TopicPartitionOffsetError;
            if (!tpo.Error.IsError && tpo.Offset.Value >= 0) result[(tpo.Topic, tpo.Partition.Value)] = tpo.Offset.Value;
        }
        return result;
    }

    public async Task<List<GroupResult>> GroupsAsync()
    {
        var ids = await DiscoverGroupsAsync();
        if (ids.Count == 0) return [];

        List<ConsumerGroupDescription> descriptions;
        try
        {
            descriptions = (await _admin.DescribeConsumerGroupsAsync(ids, new DescribeConsumerGroupsOptions { RequestTimeout = Timeout })).ConsumerGroupDescriptions;
        }
        catch (DescribeConsumerGroupsException e)
        {
            descriptions = e.Results.ConsumerGroupDescriptions;
        }

        var offsetTasks = ids.ToDictionary(id => id, id => CommittedAsync(id));
        await Task.WhenAll(offsetTasks.Values);

        var result = new List<GroupResult>();
        foreach (var id in ids)
        {
            var d = descriptions.FirstOrDefault(x => x.GroupId == id);
            var (committed, offsetsError) = offsetTasks[id].Result;
            if (d is null)
            {
                result.Add(new GroupResult(id, "Unknown", null, null, null, [], committed, offsetsError));
                continue;
            }
            if (d.State == ConsumerGroupState.Dead && (d.Members?.Count ?? 0) == 0 && (committed is null || committed.Count == 0))
                continue; // группы нет (ещё не создана или удалена)
            var members = (d.Members ?? [])
                .Select(m => new MemberView(
                    m.ClientId, m.ConsumerId, m.GroupInstanceId, m.Host,
                    (m.Assignment?.TopicPartitions ?? []).Select(tp => new TopicPartitionRef(tp.Topic, tp.Partition.Value))
                        .OrderBy(tp => tp.Topic).ThenBy(tp => tp.Partition).ToList()))
                .OrderBy(m => m.ClientId).ThenBy(m => m.MemberId)
                .ToList();
            result.Add(new GroupResult(
                id,
                d.State.ToString(),
                string.IsNullOrEmpty(d.PartitionAssignor) ? null : d.PartitionAssignor,
                d.GroupType.ToString(),
                d.Coordinator?.Id,
                members,
                committed,
                d.Error.IsError ? d.Error.Reason : offsetsError));
        }
        return result;
    }

    /// <summary>Список групп: всё, что вернул ListConsumerGroups, плюс группы стенда, известные заранее
    /// (чтобы они не «исчезали», если листинг временно не удался).</summary>
    private async Task<List<string>> DiscoverGroupsAsync()
    {
        try
        {
            var listing = await _admin.ListConsumerGroupsAsync(new ListConsumerGroupsOptions { RequestTimeout = Timeout });
            foreach (var g in listing.Valid) _knownGroups.Add(g.GroupId);
        }
        catch
        {
            // кластер частично недоступен — работаем со списком, который уже знаем
        }
        return _knownGroups.Where(id => !id.StartsWith("control-center", StringComparison.Ordinal)).OrderBy(x => x).ToList();
    }

    private async Task<(Dictionary<(string, int), long>? Committed, string? Error)> CommittedAsync(string group)
    {
        try
        {
            var res = await _offsetsAdmin.ListConsumerGroupOffsetsAsync(
                [new ConsumerGroupTopicPartitions(group, null)],
                new ListConsumerGroupOffsetsOptions { RequestTimeout = Timeout });
            return (ToDict(res[0].Partitions), null);
        }
        catch (ListConsumerGroupOffsetsException e)
        {
            var r = e.Results.FirstOrDefault();
            return (r is null ? null : ToDict(r.Partitions), r?.Error.Reason ?? e.Message);
        }
        catch (Exception e)
        {
            return (null, e.Message);
        }

        static Dictionary<(string, int), long> ToDict(List<TopicPartitionOffsetError> parts) =>
            parts.Where(p => !p.Error.IsError && p.Offset.Value >= 0)
                .ToDictionary(p => (p.Topic, p.Partition.Value), p => p.Offset.Value);
    }

    public async Task<Dictionary<string, Dictionary<string, string>>> TopicConfigsAsync(IEnumerable<string> topics)
    {
        if (Environment.TickCount64 - _configsAt < 30_000 && _topicConfigs.Count > 0) return _topicConfigs;
        try
        {
            var resources = topics.Where(t => !IsInternal(t)).Select(t => new ConfigResource { Type = ResourceType.Topic, Name = t }).ToList();
            if (resources.Count == 0) return _topicConfigs;
            var res = await _admin.DescribeConfigsAsync(resources, new DescribeConfigsOptions { RequestTimeout = Timeout });
            _topicConfigs = res.ToDictionary(
                r => r.ConfigResource.Name,
                r => r.Entries.Where(e => InterestingConfigs.Contains(e.Key)).ToDictionary(e => e.Key, e => e.Value.Value ?? ""));
            _configsAt = Environment.TickCount64;
        }
        catch
        {
            // оставляем прошлые значения
        }
        return _topicConfigs;
    }

    public void InvalidateConfigs() => _configsAt = 0;

    public void Dispose()
    {
        _offsetsAdmin.Dispose();
        _offsetsConsumer.Dispose();
        _admin.Dispose();
    }
}
