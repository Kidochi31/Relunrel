namespace Relunrel.Packets;

using System;
using System.Buffers.Binary;

internal ref struct PacketReader
{
    private ReadOnlySpan<byte> Buffer;
    private int Position;

    public PacketReader(ReadOnlySpan<byte> buffer)
    {
        Buffer = buffer;
        Position = 0;
    }

    public int Remaining => Buffer.Length - Position;

    public byte ReadByte()
    {
        return Buffer[Position++];
    }

    public ushort ReadUInt16()
    {
        ushort value =
            BinaryPrimitives.ReadUInt16BigEndian(
                Buffer.Slice(Position, 2));

        Position += 2;
        return value;
    }

    public uint ReadUInt32()
    {
        uint value =
            BinaryPrimitives.ReadUInt32BigEndian(
                Buffer.Slice(Position, 4));

        Position += 4;
        return value;
    }

    public ulong ReadUInt64()
    {
        ulong value =
            BinaryPrimitives.ReadUInt64BigEndian(
                Buffer.Slice(Position, 8));

        Position += 8;
        return value;
    }

    public ReadOnlySpan<byte> ReadBytes(int length)
    {
        ReadOnlySpan<byte> result =
            Buffer.Slice(Position, length);

        Position += length;
        return result;
    }
}