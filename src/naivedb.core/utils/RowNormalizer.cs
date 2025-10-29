using System.Text.Json;

namespace naivedb.core.utils
{
    public static class RowNormalizer
    {
        public static void NormalizeToValidTypes(this IDictionary<string, object?> row)
        {
            var keys = row.Keys.ToList();
            foreach (var key in keys)
            {
                var value = row[key];
                if (value is JsonElement json)
                    row[key] = ConvertJsonElement(json);
            }
        }

        private static object? ConvertJsonElement(JsonElement e)
        {
            return e.ValueKind switch
            {
                JsonValueKind.String => e.GetString(),
                JsonValueKind.Number when e.TryGetInt64(out var l) => l,
                JsonValueKind.Number => e.GetDouble(),
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.Null => null,
                JsonValueKind.Object => e.EnumerateObject().ToDictionary(p => p.Name, p => ConvertJsonElement(p.Value)),
                JsonValueKind.Array => e.EnumerateArray().Select(ConvertJsonElement).ToList(),
                _ => e.ToString()
            };
        }
    }
}