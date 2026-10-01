namespace ControlCenter;

public sealed class LabOptions
{
    public string BootstrapServers { get; set; } = "localhost:19092,localhost:19093,localhost:19094";
    public string ComposeProject { get; set; } = "kafka-lab";
    public string DockerNetwork { get; set; } = "kafka-lab-net";
    public string DocsPath { get; set; } = "/app/docs";

    public List<BrokerDef> Brokers { get; set; } =
    [
        new() { Id = 1, Container = "kafka-1" },
        new() { Id = 2, Container = "kafka-2" },
        new() { Id = 3, Container = "kafka-3" },
    ];

    public List<ServiceDef> Services { get; set; } =
    [
        new() { Name = "order-service", Lang = "C#", Role = "producer", Port = 8080, Topics = ["orders", "customer-profiles"] },
        new() { Name = "clickstream-generator", Lang = "Go", Role = "producer", Port = 8080, Topics = ["clickstream"] },
        new() { Name = "order-processor", Lang = "C#", Role = "consumer", Port = 8080, Group = "order-processing", Topics = ["payments", "orders.dlq"] },
        new() { Name = "analytics", Lang = "Python", Role = "consumer", Port = 8080, Group = "analytics" },
    ];
}

public sealed class BrokerDef
{
    public int Id { get; set; }
    public string Container { get; set; } = "";
}

public sealed class ServiceDef
{
    public string Name { get; set; } = "";
    public string Lang { get; set; } = "";
    public string Role { get; set; } = "";
    public int Port { get; set; } = 8080;
    /// <summary>Consumer group сервиса (если это консюмер).</summary>
    public string? Group { get; set; }
    /// <summary>В какие топики пишет сервис.</summary>
    public List<string> Topics { get; set; } = [];
}
