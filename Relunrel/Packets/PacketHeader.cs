namespace Relunrel.Packets;

internal struct PacketHeader
{
    public const int HeaderSize = 10;
    public byte ProtocolVersion;
    public uint SessionId;
    public uint ConnectionToken;
    public PacketType PacketType;

    public void Serialize(ref PacketWriter writer)
    {
        writer.WriteByte(ProtocolVersion);
        writer.WriteUInt32(SessionId);
        writer.WriteUInt32(ConnectionToken);
        writer.WriteByte((byte)PacketType);
    }

    public static PacketHeader? Deserialize(ref PacketReader reader)
    {
        if(reader.Remaining < HeaderSize)
        {
            return null;
        }

        var protocolVersion = reader.ReadByte(); // 1
        var sessionId = reader.ReadUInt32(); // 4
        var connectionToken = reader.ReadUInt32(); // 4
        var packetType = reader.ReadByte(); // 1
        if(!Enum.IsDefined(typeof(PacketType), packetType))
        {
            return null;
        }

        return new PacketHeader
        {
            ProtocolVersion = protocolVersion,
            SessionId = sessionId,
            ConnectionToken = connectionToken,
            PacketType = (PacketType)packetType
        };
    }

    public override string ToString()
    {
        return $"Header(Version={ProtocolVersion}, Session=0x{SessionId:X8}, Token=0x{ConnectionToken:X8}, Type={PacketType})";
    }

    public string ToDebugString()
    {
        return
$@"Header
{{
    ProtocolVersion = {ProtocolVersion}
    SessionId = 0x{SessionId:X8}
    ConnectionToken = 0x{ConnectionToken:X8}
    PacketType = {PacketType}
}}";
    }
}