namespace Relunrel.Channels;

internal sealed class ReliableOrderedReceiver
{
    private uint NextExpectedSequenceId;

    private readonly Dictionary<uint, byte[]> BufferedMessages = [];

    private readonly Queue<(uint SequenceId, byte[] Payload)> ReadyMessages = [];

    public bool Receive(uint sequenceId, byte[] payload)
    {
        if(SequenceNumber.IsOlder(sequenceId, NextExpectedSequenceId))
        {
            return false;
        }

        if(sequenceId == NextExpectedSequenceId)
        {
            ReadyMessages.Enqueue((sequenceId, payload));

            NextExpectedSequenceId++;

            while(BufferedMessages.Remove(NextExpectedSequenceId, out byte[]? bufferedPayload))
            {
                ReadyMessages.Enqueue((NextExpectedSequenceId, bufferedPayload));
                NextExpectedSequenceId++;
            }

            return true;
        }

        BufferedMessages.TryAdd(sequenceId, payload);

        return true;
    }

    public bool MessagesAvailable => ReadyMessages.Count > 0;

    public (uint SequenceId, byte[] Payload)? DequeueMessage()
    {
        if(ReadyMessages.TryDequeue(out (uint SequenceId, byte[] Payload) message))
        {
            return message;
        }

        return null;
    }

    public uint NextExpected => NextExpectedSequenceId;

    public int BufferedCount => BufferedMessages.Count;
}


internal sealed class UnreliableOrderedReceiver
{
    private bool HasReceivedMessage;

    private uint MostRecentSequenceId;

    private readonly Queue<(uint SequenceId, byte[] Payload)> Messages = [];

    public bool Receive(uint sequenceId, byte[] payload)
    {
        if(HasReceivedMessage && !SequenceNumber.IsNewer(sequenceId, MostRecentSequenceId))
        {
            return false;
        }

        HasReceivedMessage = true;
        MostRecentSequenceId = sequenceId;

        Messages.Enqueue((sequenceId, payload));

        return true;
    }

    public bool MessagesAvailable => Messages.Count > 0;

    public (uint SequenceId, byte[] Payload)? DequeueMessage()
    {
        if(Messages.TryDequeue(out (uint SequenceId, byte[] Payload) message))
        {
            return message;
        }

        return null;
    }
}