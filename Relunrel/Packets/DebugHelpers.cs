using System.Text;

namespace Relunrel.Packets;

internal static class DebugHelpers
{
    public static string Indent(string text, int level)
    {
        string prefix = new(' ', level * 4);

        string[] lines = text.Split('\n');

        StringBuilder builder = new();

        foreach(string line in lines)
        {
            builder.Append(prefix);
            builder.AppendLine(line);
        }

        return builder.ToString();
    }

    public static string FormatPayload(byte[] payload, int maxBytes = 32)
    {
        if(payload.Length == 0)
        {
            return "<empty>";
        }

        StringBuilder builder = new();

        int count = Math.Min(payload.Length, maxBytes);

        for(int i = 0; i < count; i++)
        {
            if(i > 0)
            {
                builder.Append(' ');
            }

            builder.Append(payload[i].ToString("X2"));
        }

        if(payload.Length > maxBytes)
        {
            builder.Append(" ...");
        }

        return builder.ToString();
    }
}