using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;

namespace ControlCenter.Docker;

public sealed record ContainerInfo(
    string Id, string Name, string Service, string State, string Status, string? Ip, long Created);

public sealed record ExecResult(long ExitCode, string StdOut, string StdErr);

/// <summary>
/// Минимальный клиент Docker Engine API поверх unix-сокета /var/run/docker.sock.
/// Через него control-center останавливает/ставит на паузу контейнеры и выполняет `tc` внутри брокеров.
/// </summary>
public sealed class DockerApi
{
    private const string SocketPath = "/var/run/docker.sock";
    private readonly HttpClient _http;
    private readonly string _network;

    public DockerApi(string network)
    {
        _network = network;
        var handler = new SocketsHttpHandler
        {
            ConnectCallback = async (_, ct) =>
            {
                var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                await socket.ConnectAsync(new UnixDomainSocketEndPoint(SocketPath), ct);
                return new NetworkStream(socket, ownsSocket: true);
            },
            PooledConnectionLifetime = TimeSpan.FromMinutes(2),
        };
        _http = new HttpClient(handler) { BaseAddress = new Uri("http://docker"), Timeout = TimeSpan.FromSeconds(40) };
    }

    public static bool Available => File.Exists(SocketPath);

    public async Task<List<ContainerInfo>> ListAsync(string project, CancellationToken ct)
    {
        var filters = JsonSerializer.Serialize(new Dictionary<string, string[]> { ["label"] = [$"com.docker.compose.project={project}"] });
        using var resp = await _http.GetAsync($"/containers/json?all=true&filters={Uri.EscapeDataString(filters)}", ct);
        resp.EnsureSuccessStatusCode();
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
        var result = new List<ContainerInfo>();
        foreach (var c in doc.RootElement.EnumerateArray())
        {
            var labels = c.GetProperty("Labels");
            var service = labels.TryGetProperty("com.docker.compose.service", out var s) ? s.GetString() ?? "" : "";
            string? ip = null;
            if (c.TryGetProperty("NetworkSettings", out var ns) && ns.TryGetProperty("Networks", out var nets) && nets.ValueKind == JsonValueKind.Object)
            {
                foreach (var n in nets.EnumerateObject())
                {
                    var addr = n.Value.TryGetProperty("IPAddress", out var a) ? a.GetString() : null;
                    if (string.IsNullOrEmpty(addr)) continue;
                    if (ip is null || n.Name == _network) ip = addr;
                }
            }
            result.Add(new ContainerInfo(
                c.GetProperty("Id").GetString()!,
                c.GetProperty("Names")[0].GetString()!.TrimStart('/'),
                service,
                c.GetProperty("State").GetString() ?? "unknown",
                c.GetProperty("Status").GetString() ?? "",
                ip,
                c.GetProperty("Created").GetInt64()));
        }
        return result;
    }

    /// <summary>Время старта контейнера (меняется после рестарта) — по нему понимаем, что tc-правила сбросились.</summary>
    public async Task<string?> StartedAtAsync(string id, CancellationToken ct)
    {
        using var resp = await _http.GetAsync($"/containers/{id}/json", ct);
        if (!resp.IsSuccessStatusCode) return null;
        using var doc = JsonDocument.Parse(await resp.Content.ReadAsStringAsync(ct));
        return doc.RootElement.GetProperty("State").GetProperty("StartedAt").GetString();
    }

    public async Task ActionAsync(string id, string action, CancellationToken ct)
    {
        var path = action switch
        {
            "start" => $"/containers/{id}/start",
            "stop" => $"/containers/{id}/stop?t=15",           // SIGTERM → корректное завершение
            "kill" => $"/containers/{id}/kill?signal=SIGKILL", // мгновенная «смерть» процесса
            "pause" => $"/containers/{id}/pause",              // заморозка (cgroup freezer) — как долгая GC-пауза
            "unpause" => $"/containers/{id}/unpause",
            "restart" => $"/containers/{id}/restart?t=15",
            _ => throw new ArgumentException($"Неизвестное действие {action}"),
        };
        using var resp = await _http.PostAsync(path, null, ct);
        if (resp.IsSuccessStatusCode || (int)resp.StatusCode == 304) return;
        throw new InvalidOperationException(await ErrorText(resp, ct));
    }

    public async Task<ExecResult> ExecAsync(string id, string[] cmd, string user = "root", string[]? env = null, CancellationToken ct = default)
    {
        using var create = await _http.PostAsJsonAsync($"/containers/{id}/exec", new
        {
            AttachStdout = true,
            AttachStderr = true,
            Tty = false,
            User = user,
            Cmd = cmd,
            Env = env ?? [],
        }, ct);
        if (!create.IsSuccessStatusCode) throw new InvalidOperationException(await ErrorText(create, ct));
        var execId = (await create.Content.ReadFromJsonAsync<JsonElement>(ct)).GetProperty("Id").GetString();

        using var start = await _http.PostAsJsonAsync($"/exec/{execId}/start", new { Detach = false, Tty = false }, ct);
        if (!start.IsSuccessStatusCode) throw new InvalidOperationException(await ErrorText(start, ct));
        var raw = await start.Content.ReadAsByteArrayAsync(ct);
        var (stdout, stderr) = Demux(raw);

        using var inspect = await _http.GetAsync($"/exec/{execId}/json", ct);
        var exit = (await inspect.Content.ReadFromJsonAsync<JsonElement>(ct)).GetProperty("ExitCode");
        return new ExecResult(exit.ValueKind == JsonValueKind.Number ? exit.GetInt64() : -1, stdout, stderr);
    }

    /// <summary>Docker мультиплексирует stdout/stderr: кадр = [тип, 0, 0, 0, размер(4 байта BE)] + данные.</summary>
    private static (string StdOut, string StdErr) Demux(byte[] raw)
    {
        var o = new StringBuilder();
        var e = new StringBuilder();
        var i = 0;
        while (i + 8 <= raw.Length)
        {
            var type = raw[i];
            var size = (raw[i + 4] << 24) | (raw[i + 5] << 16) | (raw[i + 6] << 8) | raw[i + 7];
            i += 8;
            if (size < 0 || i + size > raw.Length) break;
            var chunk = Encoding.UTF8.GetString(raw, i, size);
            (type == 2 ? e : o).Append(chunk);
            i += size;
        }
        return (o.ToString(), e.ToString());
    }

    private static async Task<string> ErrorText(HttpResponseMessage resp, CancellationToken ct)
    {
        var body = await resp.Content.ReadAsStringAsync(ct);
        try
        {
            return JsonDocument.Parse(body).RootElement.GetProperty("message").GetString() ?? body;
        }
        catch
        {
            return $"{(int)resp.StatusCode}: {body}";
        }
    }
}
