namespace Relunrel.Packets;

using System;
using System.Buffers.Binary;

internal ref struct PacketWriter
{
    private Span<byte> Buffer;
    private int Position;

    public PacketWriter(Span<byte> buffer)
    {
        Buffer = buffer;
        Position = 0;
    }

    public int Remaining => Buffer.Length - Position;
    public int BytesWritten => Position;

    public void WriteByte(byte value)
    {
        Buffer[Position++] = value;
    }

    public void WriteUInt16(ushort value)
    {
        BinaryPrimitives.WriteUInt16BigEndian(
            Buffer.Slice(Position, 2),
            value);

        Position += 2;
    }

    public void WriteUInt32(uint value)
    {
        BinaryPrimitives.WriteUInt32BigEndian(
            Buffer.Slice(Position, 4),
            value);

        Position += 4;
    }

    public void WriteUInt64(ulong value)
    {
        BinaryPrimitives.WriteUInt64BigEndian(
            Buffer.Slice(Position, 8),
            value);

        Position += 8;
    }

    public void WriteBytes(ReadOnlySpan<byte> data)
    {
        data.CopyTo(Buffer.Slice(Position));
        Position += data.Length;
    }
}