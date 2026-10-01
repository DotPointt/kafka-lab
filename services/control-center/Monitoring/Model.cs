using System.Text.Json;

namespace ControlCenter.Monitoring;

/// <summary>Состояние всего стенда раз в секунду — то, что видит браузер.</summary>
public sealed class Snapshot
{
    public long Ts { get; set; }
    public ClusterView Cluster { get; set; } = new();
    public List<BrokerView> Brokers { get; set; } = [];
    public List<TopicView> Topics { get; set; } = [];
    public List<GroupView> Groups { get; set; } = [];
    public List<ServiceView> Services { get; set; } = [];
}

public sealed class ClusterView
{
    public string? ClusterId { get; set; }
    public bool MetadataOk { get; set; }
    public string? MetadataError { get; set; }
    public bool OffsetsOk { get; set; }
    public bool GroupsOk { get; set; }
    public bool DockerOk { get; set; }
    public string? DockerError { get; set; }
    public QuorumInfo? Quorum { get; set; }
    public int BrokersAlive { get; set; }
    public int BrokersTotal { get; set; }
    public int UnderReplicated { get; set; }
    public int Offline { get; set; }
    public int UnderMinIsr { get; set; }
    public double ProduceRate { get; set; }
    public long TickMs { get; set; }
}

public sealed class QuorumInfo
{
    public int? LeaderId { get; set; }
    public List<VoterView> Voters { get; set; } = [];
    public long UpdatedAt { get; set; }
    public int? SourceBroker { get; set; }
    public string? Error { get; set; }
}

public sealed record VoterView(int Id, long LogEndOffset, long Lag, long LastFetchAgoMs, string Status);

public sealed record NetworkChaos(int LatencyMs = 0, int JitterMs = 0, double LossPct = 0, bool Isolated = false, bool SplitFromBrokers = false)
{
    public bool IsNone => LatencyMs <= 0 && LossPct <= 0 && !Isolated && !SplitFromBrokers;
}

public sealed class BrokerView
{
    public int Id { get; set; }
    public string Container { get; set; } = "";
    public string ContainerState { get; set; } = "missing";
    public string? ContainerStatus { get; set; }
    public string? Ip { get; set; }
    public bool InMetadata { get; set; }
    public bool IsQuorumLeader { get; set; }
    public VoterView? Voter { get; set; }
    public NetworkChaos Chaos { get; set; } = new();
    public int Leaders { get; set; }
    public int Replicas { get; set; }
    public double InRate { get; set; }
    /// <summary>online | fenced | paused | stopped | starting</summary>
    public string Status { get; set; } = "unknown";
}

public sealed class TopicView
{
    public string Name { get; set; } = "";
    public bool Internal { get; set; }
    public int ReplicationFactor { get; set; }
    public Dictionary<string, string> Config { get; set; } = [];
    public List<PartitionView> Partitions { get; set; } = [];
    public double Rate { get; set; }
}

public sealed class PartitionView
{
    public int Id { get; set; }
    public int Leader { get; set; }
    public int[] Replicas { get; set; } = [];
    public int[] Isr { get; set; } = [];
    public long? Start { get; set; }
    public long? End { get; set; }
    public double Rate { get; set; }
    public string? Error { get; set; }
}

public sealed record TopicPartitionRef(string Topic, int Partition);

public sealed record MemberView(string ClientId, string MemberId, string? InstanceId, string Host, List<TopicPartitionRef> Assignment);

public sealed class GroupOffsetView
{
    public string Topic { get; set; } = "";
    public int Partition { get; set; }
    public long? Committed { get; set; }
    public long? Lag { get; set; }
    public double Rate { get; set; }
}

public sealed class GroupView
{
    public string Id { get; set; } = "";
    public string State { get; set; } = "Unknown";
    public string? Assignor { get; set; }
    public string? Type { get; set; }
    public int? Coordinator { get; set; }
    public List<MemberView> Members { get; set; } = [];
    public List<GroupOffsetView> Offsets { get; set; } = [];
    public long TotalLag { get; set; }
    public double Rate { get; set; }
    public string? Error { get; set; }
}

public sealed class ServiceView
{
    public string Name { get; set; } = "";
    public string Lang { get; set; } = "";
    public string Role { get; set; } = "";
    public string? Group { get; set; }
    public List<string> Topics { get; set; } = [];
    public List<ServiceInstanceView> Instances { get; set; } = [];
}

public sealed class ServiceInstanceView
{
    public string Container { get; set; } = "";
    public string ContainerState { get; set; } = "";
    public string? Ip { get; set; }
    public bool Ok { get; set; }
    public string? Error { get; set; }
    public JsonElement? Stats { get; set; }
}

public sealed record LabEvent(long Id, long Ts, string Level, string Category, string Text, string? Learn, string? Source);
