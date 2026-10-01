using System.Text;
using System.Text.Json;
using ControlCenter.Docker;

namespace ControlCenter.Monitoring;

/// <summary>Опрашивает /api/stats всех экземпляров сервисов (адреса берём из Docker — так видны и масштабированные копии).</summary>
public sealed class ServicePoller(IHttpClientFactory httpFactory, LabOptions options)
{
    public async Task<List<ServiceView>> PollAsync(IReadOnlyList<ContainerInfo>? containers, CancellationToken ct)
    {
        var tasks = options.Services.Select(async def =>
        {
            var view = new ServiceView { Name = def.Name, Lang = def.Lang, Role = def.Role, Group = def.Group, Topics = def.Topics };
            var targets = Targets(def, containers);
            var instances = await Task.WhenAll(targets.Select(async t =>
            {
                var inst = new ServiceInstanceView { Container = t.Container, ContainerState = t.State, Ip = t.Host };
                if (t.State != "running") return inst;
                try
                {
                    using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                    cts.CancelAfter(1500);
                    var http = httpFactory.CreateClient("services");
                    var json = await http.GetStringAsync($"http://{t.Host}:{def.Port}/api/stats", cts.Token);
                    inst.Stats = JsonDocument.Parse(json).RootElement.Clone();
                    inst.Ok = true;
                }
                catch (Exception ex)
                {
                    inst.Error = ex is OperationCanceledException ? "таймаут" : ex.Message;
                }
                return inst;
            }));
            view.Instances = instances.ToList();
            return view;
        });
        return (await Task.WhenAll(tasks)).ToList();
    }

    private static List<(string Container, string State, string Host)> Targets(ServiceDef def, IReadOnlyList<ContainerInfo>? containers)
    {
        if (containers is null) return [(def.Name, "running", def.Name)]; // Docker недоступен — идём по DNS-имени
        return containers.Where(c => c.Service == def.Name)
            .OrderBy(c => c.Name)
            .Select(c => (c.Name, c.State, c.Ip ?? def.Name))
            .ToList();
    }

    /// <summary>Проксирует запрос UI к сервису. Изменения конфигурации рассылаются всем экземплярам.</summary>
    public async Task<IResult> ProxyAsync(string service, string path, HttpRequest request, IReadOnlyList<ContainerInfo>? containers, CancellationToken ct)
    {
        var def = options.Services.FirstOrDefault(s => s.Name == service);
        if (def is null) return Results.NotFound(new { error = $"Нет сервиса {service}" });
        var targets = Targets(def, containers).Where(t => t.State == "running").ToList();
        if (targets.Count == 0) return Results.Json(new { ok = false, error = $"{service} не запущен" }, statusCode: 503);

        string body;
        using (var reader = new StreamReader(request.Body, Encoding.UTF8)) body = await reader.ReadToEndAsync(ct);
        var broadcast = path.StartsWith("config", StringComparison.Ordinal);
        if (!broadcast) targets = targets.Take(1).ToList();

        var http = httpFactory.CreateClient("services");
        string? firstBody = null;
        var status = 200;
        foreach (var t in targets)
        {
            var msg = new HttpRequestMessage(new HttpMethod(request.Method), $"http://{t.Host}:{def.Port}/api/{path}{request.QueryString}");
            if (request.Method is "POST" or "PUT" or "PATCH")
                msg.Content = new StringContent(string.IsNullOrEmpty(body) ? "{}" : body, Encoding.UTF8, "application/json");
            try
            {
                using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                cts.CancelAfter(TimeSpan.FromSeconds(40));
                using var resp = await http.SendAsync(msg, cts.Token);
                var text = await resp.Content.ReadAsStringAsync(cts.Token);
                firstBody ??= text;
                if (!resp.IsSuccessStatusCode) status = (int)resp.StatusCode;
            }
            catch (Exception ex)
            {
                firstBody ??= JsonSerializer.Serialize(new { ok = false, error = ex.Message });
                status = 502;
            }
        }
        return Results.Content(firstBody ?? "{}", "application/json", Encoding.UTF8, status);
    }
}
