namespace Relunrel.Packets;

internal enum RecordType : byte
{
    UnreliableUnordered = 0,
    UnreliableOrdered = 1,
    ReliableUnordered = 2,
    ReliableOrdered = 3,

    AckMaskReliableUnordered = 4,
    AckMaskReliableOrdered = 5,

    AckContiguousReliableUnordered = 6,
    AckContiguousReliableOrdered = 7
}