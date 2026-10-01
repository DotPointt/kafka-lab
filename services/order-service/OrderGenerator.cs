using System.Diagnostics;

namespace OrderService;

/// <summary>Генератор нагрузки: равномерно выпускает RatePerSec заказов в секунду.</summary>
public sealed class OrderGenerator(OrderProducer producer) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        var clock = Stopwatch.StartNew();
        var last = clock.Elapsed.TotalSeconds;
        double budget = 0;

        while (!ct.IsCancellationRequested)
        {
            await Task.Delay(10, ct);
            var now = clock.Elapsed.TotalSeconds;
            var rate = producer.Settings.RatePerSec;
            budget += rate * (now - last);
            last = now;

            if (rate <= 0)
            {
                budget = 0;
                continue;
            }

            // Не пытаемся «догнать» больше секунды пропущенной нагрузки.
            budget = Math.Min(budget, Math.Max(rate, 1));
            while (budget >= 1)
            {
                if (!producer.ProduceGenerated())
                {
                    budget = 0; // локальный буфер полон — ждём следующего тика (backpressure)
                    break;
                }
                budget -= 1;
            }
        }
    }
}
