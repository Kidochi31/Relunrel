using System.Text;

namespace Relunrel.Packets;

internal abstract class PacketBody
{
    public abstract void Serialize(ref PacketWriter writer);

    public abstract string ToDebugString();
}

internal sealed class MessagePacket : PacketBody
{
    public const int MaximumBodySize = Packet.MaximumPacketSize - PacketHeader.HeaderSize;

    public List<Record> Records = new();
    public int SerializedSize {get; private set;} = 0 ;

    public bool AddRecord(Record record)
    {
        if(SerializedSize + record.GetSerializedSize() + 1 > MaximumBodySize)
        {
            return false;
        }
        Records.Add(record);
        SerializedSize += record.GetSerializedSize() + 1; // one extra for record type
        return true;
    }

    public override void Serialize(ref PacketWriter writer)
    {
        foreach (Record record in Records)
        {
            record.Serialize(ref writer);
        }
    }

    public static MessagePacket? Deserialize(ref PacketReader reader)
    {
        MessagePacket packet = new MessagePacket();

        while (reader.Remaining > 0)
        {
            Record? newRecord = Record.Deserialize(ref reader);
            if(newRecord is null)
            {
                return null;
            }
            packet.Records.Add(newRecord);
            packet.SerializedSize += newRecord.GetSerializedSize() + 1; // one extra for record type
        }

        return packet;
    }

    public override string ToString()
    {
        StringBuilder builder = new();

        builder.AppendLine("MessagePacket");
        builder.AppendLine("{");
        builder.AppendLine($"    Records = {Records.Count}");

        foreach(Record record in Records)
        {
            builder.AppendLine($"    {record}");
        }

        builder.Append('}');

        return builder.ToString();
    }

    public override string ToDebugString()
    {
        StringBuilder builder = new();

        builder.AppendLine("MessagePacket");
        builder.AppendLine("{");
        builder.AppendLine($"    RecordCount = {Records.Count}");
        builder.AppendLine();

        foreach(Record record in Records)
        {
            builder.Append(
                DebugHelpers.Indent(
                    record.ToDebugString(),
                    1));

            builder.AppendLine();
        }

        builder.Append('}');

        return builder.ToString();
    }
}