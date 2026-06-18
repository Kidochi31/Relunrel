namespace Relunrel.Channels;

internal sealed class PendingSequencedMessage
{
    public uint SequenceId;
    public byte[] Payload = Array.Empty<byte>();
}