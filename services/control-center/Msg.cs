namespace ControlCenter;

/// <summary>Сообщение на двух языках: UI показывает версию на выбранном языке (ru / en).</summary>
public sealed record Msg(string Ru, string En)
{
    /// <summary>Тело ответа API с ошибкой: error — по-русски, errorEn — по-английски.</summary>
    public object Error() => new { ok = false, error = Ru, errorEn = En };
}
