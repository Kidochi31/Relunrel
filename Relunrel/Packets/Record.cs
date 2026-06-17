namespace Relunrel.Packets;

internal abstract class Record
{
    public const int MaximumPayloadSize = 1024;

    public abstract RecordType Type { get; }

    public abstract void Serialize(ref PacketWriter writer);

    // Excludes the RECORD_TYPE byte
    public abstract int GetSerializedSize();

    public static Record? Deserialize(ref PacketReader reader)
    {
        if(reader.Remaining < 1)
        {
            return null;
        }
        byte recordType = reader.ReadByte();
        if(!Enum.IsDefined(typeof(RecordType), recordType))
        {
            return null;
        }
        RecordType type = (RecordType)recordType;

        return type switch
        {
            RecordType.UnreliableUnordered =>
                UnreliableUnorderedRecord.Deserialize(ref reader),

            RecordType.UnreliableOrdered =>
                UnreliableOrderedRecord.Deserialize(ref reader),

            RecordType.ReliableUnordered =>
                ReliableUnorderedRecord.Deserialize(ref reader),

            RecordType.ReliableOrdered =>
                ReliableOrderedRecord.Deserialize(ref reader),

            RecordType.AckMaskReliableUnordered =>
                AckMaskRecord.Deserialize(
                    RecordType.AckMaskReliableUnordered,
                    ref reader),

            RecordType.AckMaskReliableOrdered =>
                AckMaskRecord.Deserialize(
                    RecordType.AckMaskReliableOrdered,
                    ref reader),

            RecordType.AckContiguousReliableUnordered =>
                AckContiguousRecord.Deserialize(
                    RecordType.AckContiguousReliableUnordered,
                    ref reader),

            RecordType.AckContiguousReliableOrdered =>
                AckContiguousRecord.Deserialize(
                    RecordType.AckContiguousReliableOrdered,
                    ref reader),

            _ => null
        };
    }

    public abstract string ToDebugString();
}


internal sealed class UnreliableUnorderedRecord : Record
{
    public readonly ushort ChannelId;
    public readonly byte[] Payload = Array.Empty<byte>();

    public override RecordType Type =>
        RecordType.UnreliableUnordered;

    public override void Serialize(ref PacketWriter writer)
    {
        writer.WriteByte((byte)Type);
        writer.WriteUInt16(ChannelId);
        writer.WriteUInt16((ushort)Payload.Length);
        writer.WriteBytes(Payload);
    }

    private UnreliableUnorderedRecord(ushort channelId, byte[] payload)
    {
        ChannelId = channelId;
        Payload = payload;
    }

    public static UnreliableUnorderedRecord? Create(ushort channelId, byte[] payload)
    {
        if(payload.Length > MaximumPayloadSize)
        {
            return null;
        }
        return new UnreliableUnorderedRecord(channelId, payload);
    }

    public new static UnreliableUnorderedRecord? Deserialize(ref PacketReader reader)
    {
        if(reader.Remaining < 4)
        {
            return null;
        }

        ushort channelId = reader.ReadUInt16(); // 2
        ushort length = reader.ReadUInt16(); // 2

        if(reader.Remaining < length || length > MaximumPayloadSize)
        {
            return null;
        }
        byte[] payload = reader.ReadBytes(length).ToArray();

        return new UnreliableUnorderedRecord(channelId, payload);
    }

    public override int GetSerializedSize() => 4 + Payload.Length;

    public override string ToString()
    {
        return $"UnreliableUnordered(Channel={ChannelId}, Length={Payload.Length}, Payload=[{DebugHelpers.FormatPayload(Payload)}])";
    }

    public override string ToDebugString()
    {
        return
$@"UnreliableUnordered
{{
    ChannelId = {ChannelId}
    PayloadLength = {Payload.Length}
    Payload = {DebugHelpers.FormatPayload(Payload)}
}}";
    }
}

internal abstract class SequencedRecord : Record
{
    public readonly ushort ChannelId;
    public readonly uint SequenceId;
    public readonly byte[] Payload = Array.Empty<byte>();

    protected SequencedRecord(ushort channelId, uint sequenceId, byte[] payload)
    {
        ChannelId = channelId;
        SequenceId = sequenceId;
        Payload = payload;
    }

    public override void Serialize(ref PacketWriter writer)
    {
        writer.WriteByte((byte)Type);
        writer.WriteUInt16(ChannelId);
        writer.WriteUInt32(SequenceId);
        writer.WriteUInt16((ushort)Payload.Length);
        writer.WriteBytes(Payload);
    }

    protected static (ushort ChannelId, uint SequenceId, byte[] Payload)? DeserializeBody(ref PacketReader reader)
    {
        if(reader.Remaining < 8)
        {
            return null;
        }

        ushort channelId = reader.ReadUInt16(); // 2
        uint sequenceId = reader.ReadUInt32(); // 4

        ushort length = reader.ReadUInt16(); // 2

        if(reader.Remaining < length || length > MaximumPayloadSize)
        {
            return null;
        }
        byte[] payload = reader.ReadBytes(length).ToArray();

        return (channelId, sequenceId, payload);
    }

    public override int GetSerializedSize() => 8 + Payload.Length;
}

internal sealed class ReliableOrderedRecord : SequencedRecord
{
    public override RecordType Type => RecordType.ReliableOrdered;

    private ReliableOrderedRecord(ushort channelId, uint sequenceId, byte[] payload) : base(channelId, sequenceId, payload)
    {
    }

    public static ReliableOrderedRecord? Create(ushort channelId, uint sequenceId, byte[] payload)
    {
        if(payload.Length > MaximumPayloadSize)
        {
            return null;
        }
        return new ReliableOrderedRecord(channelId, sequenceId, payload);
    }

    public new static ReliableOrderedRecord? Deserialize(ref PacketReader reader)
    {
        var body = DeserializeBody(ref reader);
        if(body is null)
        {
            return null;
        }

        return new ReliableOrderedRecord(body.Value.ChannelId, body.Value.SequenceId, body.Value.Payload);
    }

    public override string ToString()
    {
        return $"ReliableOrdered(Channel={ChannelId}, Seq={SequenceId}, Length={Payload.Length}, Payload=[{DebugHelpers.FormatPayload(Payload)}])";
    }

    public override string ToDebugString()
    {
        return
$@"ReliableOrdered
{{
    ChannelId = {ChannelId}
    SequenceId = {SequenceId}
    PayloadLength = {Payload.Length}
    Payload = {DebugHelpers.FormatPayload(Payload)}
}}";
    }       
}

internal sealed class ReliableUnorderedRecord : SequencedRecord
{
    public override RecordType Type => RecordType.ReliableUnordered;

    private ReliableUnorderedRecord(ushort channelId, uint sequenceId, byte[] payload) : base(channelId, sequenceId, payload)
    {
    }

    public static ReliableUnorderedRecord? Create(ushort channelId, uint sequenceId, byte[] payload)
    {
        if(payload.Length > MaximumPayloadSize)
        {
            return null;
        }
        return new ReliableUnorderedRecord(channelId, sequenceId, payload);
    }

    public new static ReliableUnorderedRecord? Deserialize(ref PacketReader reader)
    {
        var body = DeserializeBody(ref reader);
        if(body is null)
        {
            return null;
        }

        return new ReliableUnorderedRecord(body.Value.ChannelId, body.Value.SequenceId, body.Value.Payload);
    }

    public override string ToString()
    {
        return $"ReliableUnordered(Channel={ChannelId}, Seq={SequenceId}, Length={Payload.Length}, Payload=[{DebugHelpers.FormatPayload(Payload)}])";
    }

    public override string ToDebugString()
    {
        return
$@"ReliableUnordered
{{
    ChannelId = {ChannelId}
    SequenceId = {SequenceId}
    PayloadLength = {Payload.Length}
    Payload = {DebugHelpers.FormatPayload(Payload)}
}}";
    }     
}

internal sealed class UnreliableOrderedRecord : SequencedRecord
{
    public override RecordType Type => RecordType.UnreliableOrdered;

    private UnreliableOrderedRecord(ushort channelId, uint sequenceId, byte[] payload) : base(channelId, sequenceId, payload)
    {
    }

    public static UnreliableOrderedRecord? Create(ushort channelId, uint sequenceId, byte[] payload)
    {
        if(payload.Length > MaximumPayloadSize)
        {
            return null;
        }
        return new UnreliableOrderedRecord(channelId, sequenceId, payload);
    }

    public new static UnreliableOrderedRecord? Deserialize(ref PacketReader reader)
    {
        var body = DeserializeBody(ref reader);
        if(body is null)
        {
            return null;
        }

        return new UnreliableOrderedRecord(body.Value.ChannelId, body.Value.SequenceId, body.Value.Payload);
    }

    public override string ToString()
    {
        return $"UnreliableOrdered(Channel={ChannelId}, Seq={SequenceId}, Length={Payload.Length}, Payload=[{DebugHelpers.FormatPayload(Payload)}])";
    }

    public override string ToDebugString()
    {
        return
$@"UnreliableOrdered
{{
    ChannelId = {ChannelId}
    SequenceId = {SequenceId}
    PayloadLength = {Payload.Length}
    Payload = {DebugHelpers.FormatPayload(Payload)}
}}";
    }   
}

internal sealed class AckMaskRecord : Record
{
    public readonly RecordType _type;

    public ushort ChannelId;
    public uint RelativeSequenceId;
    public ulong AckBitfield;

    public AckMaskRecord(RecordType type, ushort channelId, uint relativeSequenceId, ulong ackBitField)
    {
        _type = type;
        ChannelId = channelId;
        RelativeSequenceId = relativeSequenceId;
        AckBitfield = ackBitField;
    }

    public override RecordType Type => _type;

    public override void Serialize(ref PacketWriter writer)
    {
        writer.WriteByte((byte)Type);
        writer.WriteUInt16(ChannelId);
        writer.WriteUInt32(RelativeSequenceId);
        writer.WriteUInt64(AckBitfield);
    }

    public static AckMaskRecord? Deserialize(RecordType type, ref PacketReader reader)
    {
        if(reader.Remaining < 14)
        {
            return null;
        }

        var ChannelId = reader.ReadUInt16(); // 2
        var RelativeSequenceId = reader.ReadUInt32(); // 4
        var AckBitfield = reader.ReadUInt64(); // 8
        return new AckMaskRecord(type, ChannelId, RelativeSequenceId, AckBitfield);
    }

    public override int GetSerializedSize() => 14;

    public override string ToString()
    {
        return $"{Type}(Channel={ChannelId}, RelativeSeq={RelativeSequenceId}, Mask=0x{AckBitfield:X16})";
    }

    public override string ToDebugString()
    {
        return
$@"{Type}
{{
    ChannelId = {ChannelId}
    RelativeSequenceId = {RelativeSequenceId}
    AckBitfield = 0x{AckBitfield:X16}
}}";
    }
}

internal sealed class AckContiguousRecord : Record
{
    public readonly RecordType _type;

    public ushort ChannelId;
    public uint AcknowledgedSequenceId;

    public AckContiguousRecord(RecordType type, ushort channelId, uint acknowledgedSequenceId)
    {
        _type = type;
        ChannelId = channelId;
        AcknowledgedSequenceId = acknowledgedSequenceId;
    }

    public override RecordType Type => _type;

    public override void Serialize(ref PacketWriter writer)
    {
        writer.WriteByte((byte)Type);
        writer.WriteUInt16(ChannelId);
        writer.WriteUInt32(AcknowledgedSequenceId);
    }

    public static AckContiguousRecord? Deserialize(RecordType type, ref PacketReader reader)
    {
        if(reader.Remaining < 6)
        {
            return null;
        }

        var ChannelId = reader.ReadUInt16(); // 2
        var AcknowledgedSequenceId = reader.ReadUInt32(); // 4

        return new AckContiguousRecord(type, ChannelId, AcknowledgedSequenceId);
    }

    public override int GetSerializedSize() => 6;

    public override string ToString()
    {
        return $"{Type}(Channel={ChannelId}, Ack={AcknowledgedSequenceId})";
    }

    public override string ToDebugString()
    {
        return
$@"{Type}
{{
    ChannelId = {ChannelId}
    AcknowledgedSequenceId = {AcknowledgedSequenceId}
}}";
    }
}