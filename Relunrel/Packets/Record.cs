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
    public readonly byte[] Payload = Array.Empty<byte>();

    public override RecordType Type =>
        RecordType.UnreliableUnordered;

    public override void Serialize(ref PacketWriter writer)
    {
        writer.WriteByte((byte)Type);
        writer.WriteUInt16((ushort)Payload.Length);
        writer.WriteBytes(Payload);
    }

    private UnreliableUnorderedRecord(byte[] payload)
    {
        Payload = payload;
    }

    public static UnreliableUnorderedRecord? Create(byte[] payload)
    {
        if(payload.Length > MaximumPayloadSize)
        {
            return null;
        }
        return new UnreliableUnorderedRecord(payload);
    }

    public new static UnreliableUnorderedRecord? Deserialize(ref PacketReader reader)
    {
        if(reader.Remaining < 2)
        {
            return null;
        }
        ushort length = reader.ReadUInt16(); // 2

        if(reader.Remaining < length || length > MaximumPayloadSize)
        {
            return null;
        }
        byte[] payload = reader.ReadBytes(length).ToArray();

        return new UnreliableUnorderedRecord(payload);
    }

    public override int GetSerializedSize() => 2 + Payload.Length;

    public override string ToString()
    {
        return $"UnreliableUnordered(Length={Payload.Length}, Payload=[{DebugHelpers.FormatPayload(Payload)}])";
    }

    public override string ToDebugString()
    {
        return
$@"UnreliableUnordered
{{
    PayloadLength = {Payload.Length}
    Payload = {DebugHelpers.FormatPayload(Payload)}
}}";
    }
}

internal abstract class SequencedRecord : Record
{
    public readonly uint SequenceId;
    public readonly byte[] Payload = Array.Empty<byte>();

    protected SequencedRecord(uint sequenceId, byte[] payload)
    {
        SequenceId = sequenceId;
        Payload = payload;
    }

    public override void Serialize(ref PacketWriter writer)
    {
        writer.WriteByte((byte)Type);
        writer.WriteUInt32(SequenceId);
        writer.WriteUInt16((ushort)Payload.Length);
        writer.WriteBytes(Payload);
    }

    protected static (uint SequenceId, byte[] Payload)? DeserializeBody(ref PacketReader reader)
    {
        if(reader.Remaining < 6)
        {
            return null;
        }
        uint sequenceId = reader.ReadUInt32(); // 4

        ushort length = reader.ReadUInt16(); // 2

        if(reader.Remaining < length || length > MaximumPayloadSize)
        {
            return null;
        }
        byte[] payload = reader.ReadBytes(length).ToArray();

        return (sequenceId, payload);
    }

    public override int GetSerializedSize() => 6 + Payload.Length;
}

internal sealed class ReliableOrderedRecord : SequencedRecord
{
    public override RecordType Type => RecordType.ReliableOrdered;

    private ReliableOrderedRecord(uint sequenceId, byte[] payload) : base(sequenceId, payload)
    {
    }

    public static ReliableOrderedRecord? Create(uint sequenceId, byte[] payload)
    {
        if(payload.Length > MaximumPayloadSize)
        {
            return null;
        }
        return new ReliableOrderedRecord(sequenceId, payload);
    }

    public new static ReliableOrderedRecord? Deserialize(ref PacketReader reader)
    {
        var body = DeserializeBody(ref reader);
        if(body is null)
        {
            return null;
        }

        return new ReliableOrderedRecord(body.Value.SequenceId, body.Value.Payload);
    }

    public override string ToString()
    {
        return $"ReliableOrdered(Seq={SequenceId}, Length={Payload.Length}, Payload=[{DebugHelpers.FormatPayload(Payload)}])";
    }

    public override string ToDebugString()
    {
        return
$@"ReliableOrdered
{{
    SequenceId = {SequenceId}
    PayloadLength = {Payload.Length}
    Payload = {DebugHelpers.FormatPayload(Payload)}
}}";
    }       
}

internal sealed class ReliableUnorderedRecord : SequencedRecord
{
    public override RecordType Type => RecordType.ReliableUnordered;

    private ReliableUnorderedRecord(uint sequenceId, byte[] payload) : base(sequenceId, payload)
    {
    }

    public static ReliableUnorderedRecord? Create(uint sequenceId, byte[] payload)
    {
        if(payload.Length > MaximumPayloadSize)
        {
            return null;
        }
        return new ReliableUnorderedRecord(sequenceId, payload);
    }

    public new static ReliableUnorderedRecord? Deserialize(ref PacketReader reader)
    {
        var body = DeserializeBody(ref reader);
        if(body is null)
        {
            return null;
        }

        return new ReliableUnorderedRecord(body.Value.SequenceId, body.Value.Payload);
    }

    public override string ToString()
    {
        return $"ReliableUnordered(Seq={SequenceId}, Length={Payload.Length}, Payload=[{DebugHelpers.FormatPayload(Payload)}])";
    }

    public override string ToDebugString()
    {
        return
$@"ReliableUnordered
{{
    SequenceId = {SequenceId}
    PayloadLength = {Payload.Length}
    Payload = {DebugHelpers.FormatPayload(Payload)}
}}";
    }     
}

internal sealed class UnreliableOrderedRecord : SequencedRecord
{
    public override RecordType Type => RecordType.UnreliableOrdered;

    private UnreliableOrderedRecord(uint sequenceId, byte[] payload) : base(sequenceId, payload)
    {
    }

    public static UnreliableOrderedRecord? Create(uint sequenceId, byte[] payload)
    {
        if(payload.Length > MaximumPayloadSize)
        {
            return null;
        }
        return new UnreliableOrderedRecord(sequenceId, payload);
    }

    public new static UnreliableOrderedRecord? Deserialize(ref PacketReader reader)
    {
        var body = DeserializeBody(ref reader);
        if(body is null)
        {
            return null;
        }

        return new UnreliableOrderedRecord(body.Value.SequenceId, body.Value.Payload);
    }

    public override string ToString()
    {
        return $"UnreliableOrdered(Seq={SequenceId}, Length={Payload.Length}, Payload=[{DebugHelpers.FormatPayload(Payload)}])";
    }

    public override string ToDebugString()
    {
        return
$@"UnreliableOrdered
{{
    SequenceId = {SequenceId}
    PayloadLength = {Payload.Length}
    Payload = {DebugHelpers.FormatPayload(Payload)}
}}";
    }   
}

internal sealed class AckMaskRecord : Record
{
    public readonly RecordType _type;

    public uint RelativeSequenceId;
    public ulong AckBitfield;

    public AckMaskRecord(RecordType type, uint relativeSequenceId, ulong ackBitField)
    {
        _type = type;
        RelativeSequenceId = relativeSequenceId;
        AckBitfield = ackBitField;
    }

    public override RecordType Type => _type;

    public override void Serialize(ref PacketWriter writer)
    {
        writer.WriteByte((byte)Type);
        writer.WriteUInt32(RelativeSequenceId);
        writer.WriteUInt64(AckBitfield);
    }

    public static AckMaskRecord? Deserialize(RecordType type, ref PacketReader reader)
    {
        if(reader.Remaining < 12)
        {
            return null;
        }

        var RelativeSequenceId = reader.ReadUInt32(); // 4
        var AckBitfield = reader.ReadUInt64(); // 8
        return new AckMaskRecord(type, RelativeSequenceId, AckBitfield);
    }

    public override int GetSerializedSize() => 12;

    public override string ToString()
    {
        return $"{Type}(RelativeSeq={RelativeSequenceId}, Mask=0x{AckBitfield:X16})";
    }

    public override string ToDebugString()
    {
        return
$@"{Type}
{{
    RelativeSequenceId = {RelativeSequenceId}
    AckBitfield = 0x{AckBitfield:X16}
}}";
    }
}

internal sealed class AckContiguousRecord : Record
{
    public readonly RecordType _type;

    public uint AcknowledgedSequenceId;

    public AckContiguousRecord(RecordType type, uint acknowledgedSequenceId)
    {
        _type = type;
        AcknowledgedSequenceId = acknowledgedSequenceId;
    }

    public override RecordType Type => _type;

    public override void Serialize(ref PacketWriter writer)
    {
        writer.WriteByte((byte)Type);
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

        return new AckContiguousRecord(type, AcknowledgedSequenceId);
    }

    public override int GetSerializedSize() => 6;

    public override string ToString()
    {
        return $"{Type}(Ack={AcknowledgedSequenceId})";
    }

    public override string ToDebugString()
    {
        return
$@"{Type}
{{
    AcknowledgedSequenceId = {AcknowledgedSequenceId}
}}";
    }
}