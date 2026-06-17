namespace Relunrel.Packets;

internal enum PacketType : byte
{
    RequestConnect = 0,
    RespondConnect = 1,
    CompleteConnect = 2,
    Reset = 3,
    RequestFin = 4,
    RespondFin = 5,
    CompleteFin = 6,
    Message = 7,
    Heartbeat = 8
}