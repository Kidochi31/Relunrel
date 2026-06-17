using System.Text;

namespace Relunrel.Packets;

internal class Packet
{
    public const int MaximumPacketSize = 1200;

    public PacketHeader Header;
    public PacketBody? Body;

    public byte[] Serialize()
    {
        byte[] buffer = new byte[MaximumPacketSize];

        var writer = new PacketWriter(buffer);

        Header.Serialize(ref writer);

        if(Body is not null)
        {
            Body.Serialize(ref writer);
        }

        return buffer[..writer.BytesWritten];
    }

    public static Packet? Deserialize(ReadOnlySpan<byte> data)
    {
        Packet packet = new();

        var reader = new PacketReader(data);

        PacketHeader? header = PacketHeader.Deserialize(ref reader);
        if(header is null)
        {
            return null;
        }

        packet.Header = header.Value;
        if(packet.Header.PacketType == PacketType.Message)
        {
            PacketBody? message = MessagePacket.Deserialize(ref reader);
            if(message is null)
            {
                return null;
            }
            packet.Body = message;
        }
        else if (reader.Remaining > 0)
        {
            return null;
        }

        return packet;
    }

    public override string ToString()
    {
        if(Body == null)
        {
            return $"Packet {{ {Header} }}";
        }

        return $"Packet {{ {Header}, Body={Body} }}";
    }

    public string ToDebugString()
    {
        StringBuilder builder = new();

        builder.AppendLine("Packet");
        builder.AppendLine("{");

        builder.Append(
            DebugHelpers.Indent(
                Header.ToDebugString(),
                1));

        if(Body != null)
        {
            builder.AppendLine();

            builder.Append(
                DebugHelpers.Indent(
                    Body.ToDebugString(),
                    1));
        }

        builder.AppendLine("}");

        return builder.ToString();
    }
}
