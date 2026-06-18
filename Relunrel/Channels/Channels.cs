using Relunrel.Connections;
using Relunrel.Packets;
namespace Relunrel.Channels;

internal sealed class ReliableUnorderedChannel
{

    private uint NextSequenceId;

    private readonly Dictionary<uint, PendingSequencedMessage> PendingMessages = new();

    private readonly HashSet<uint> ReceivedMessages = new();

    private readonly Queue<byte[]> ReceivedPayloads = new();

    private uint HighestContiguousSequenceId;

    private bool HasReceivedMessages;

    public int PendingMessageCount => PendingMessages.Count;

    public int ReceivedMessageCount => ReceivedPayloads.Count;


    public ReliableUnorderedRecord? Send(byte[] payload)
    {
        if(payload.Length > Constants.MaxMessageSize)
        {
            return null;
        }
        uint sequenceId = NextSequenceId++;

        PendingMessages.Add(sequenceId,
            new PendingSequencedMessage
            {
                SequenceId = sequenceId,
                Payload = payload
            });

        return ReliableUnorderedRecord.Create(sequenceId, payload)!;
    }

    public void HandleReliableMessage(ReliableUnorderedRecord record)
    {
        if(ReceivedMessages.Contains(record.SequenceId))
        {
            return;
        }

        ReceivedMessages.Add(record.SequenceId);

        ReceivedPayloads.Enqueue(record.Payload);

        UpdateContiguousSequenceId(record.SequenceId);
    }

    private void UpdateContiguousSequenceId(uint sequenceId)
    {
        if(!HasReceivedMessages)
        {
            HighestContiguousSequenceId = sequenceId;
            HasReceivedMessages = true;
            return;
        }

        while(ReceivedMessages.Contains(HighestContiguousSequenceId + 1))
        {
            HighestContiguousSequenceId++;
        }
    }

    public AckContiguousRecord BuildAck()
    {
        return new AckContiguousRecord(
            RecordType.AckContiguousReliableUnordered,
            HighestContiguousSequenceId);
    }

    public void HandleAck(AckContiguousRecord record)
    {
        List<uint> acknowledged = new();

        foreach(uint sequenceId in PendingMessages.Keys)
        {
            if(sequenceId == record.AcknowledgedSequenceId ||
            SequenceNumber.IsOlder(sequenceId, record.AcknowledgedSequenceId))
            {
                acknowledged.Add(sequenceId);
            }
        }

        foreach(uint sequenceId in acknowledged)
        {
            PendingMessages.Remove(sequenceId);
        }
    }

    public bool TryDequeueMessage(out byte[] payload)
    {
        return ReceivedPayloads.TryDequeue(out payload!);
    }

    
}