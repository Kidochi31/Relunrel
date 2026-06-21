using Relunrel.Packets;

namespace Relunrel.Channels;

internal sealed class PendingMessage
{
    public required uint SequenceId { get; init; }

    public required byte[] Payload { get; init; }

    public required DateTime FirstSendTime { get; set; }

    public required DateTime LastSendTime { get; set; }

    public int RetransmissionCount { get; set; }
}

internal sealed class ReliableSender
{
    private uint NextSequenceId;

    private readonly Dictionary<uint, PendingMessage> PendingMessages = [];

    public uint PendingMessageCount => (uint)PendingMessages.Count;

    private readonly Queue<PendingMessage> RetransmissionQueue = [];

    public TimeSpan RetransmissionTimeout { get; set; } = TimeSpan.FromMilliseconds(500);

    public int MaximumRetransmissions { get; set; } = 8;

    public bool ResetRequired { get; private set; }

    public bool RetransmissionsAvailable => RetransmissionQueue.Count > 0;
    public TimeSpan? EstimatedRtt { get; private set; }

    public ReliableSender()
    {
        NextSequenceId = 0;
    }

    public void Tick(DateTime time)
    {
        foreach(PendingMessage message in PendingMessages.Values)
        {
            if(time - message.LastSendTime < RetransmissionTimeout)
            {
                continue;
            }

            message.LastSendTime = time;

            message.RetransmissionCount++;

            RetransmissionQueue.Enqueue(message);

            if(message.RetransmissionCount > MaximumRetransmissions)
            {
                ResetRequired = true;
            }
        }
    }

    public uint Send(byte[] payload, DateTime time)
    {
        uint sequenceId = NextSequenceId++;

        PendingMessages.Add(
            sequenceId,
            new PendingMessage
            {
                SequenceId = sequenceId,
                Payload = payload,
                FirstSendTime = time,
                LastSendTime = time,
                RetransmissionCount = 0
            });

        return sequenceId;
    }

    public PendingMessage? DequeueRetransmission()
    {
        if(RetransmissionQueue.TryDequeue(out PendingMessage? message))
        {
            return message;
        }

        return null;
    }

    public PendingMessage? GetPendingMessage(uint sequenceId)
    {
        if (!HasPendingMessage(sequenceId))
        {
            return null;
        }
        return PendingMessages[sequenceId];
    }

    public bool HasPendingMessage(uint sequenceId)
    {
        return PendingMessages.ContainsKey(sequenceId);
    }

    public void HandleAcknowledgementContiguous(uint acknowledgedSequenceId, DateTime time)
    {
        List<uint> remove = [];

        foreach(uint sequenceId in PendingMessages.Keys)
        {
            if(sequenceId == acknowledgedSequenceId || SequenceNumber.IsOlder(sequenceId, acknowledgedSequenceId))
            {
                remove.Add(sequenceId);
            }
        }

        foreach(uint sequenceId in remove)
        {
            UpdateRtt(sequenceId, time);
            PendingMessages.Remove(sequenceId);
        }
    }

    public void HandleAcknowledgementMask(uint relativeSequenceId, ulong acknowledgementMask, DateTime time)
    {
        for(int i = 0; i < 64; i++)
        {
            if((acknowledgementMask & (1UL << i)) == 0)
            {
                continue;
            }

            uint sequenceId = relativeSequenceId - (uint)i;
            UpdateRtt(sequenceId, time);
            PendingMessages.Remove(sequenceId);
        }
    }

    public IEnumerable<PendingMessage> Pending()
    {
        return PendingMessages.Values;
    }

    private void UpdateRtt(uint sequenceId, DateTime time)
    {
        if (!PendingMessages.ContainsKey(sequenceId))
        {
            return;
        }
        PendingMessage message = PendingMessages[sequenceId];
        if(message.RetransmissionCount > 0)
        {
            return;
        }

        TimeSpan sample = time - message.FirstSendTime;

        if(EstimatedRtt == null)
        {
            EstimatedRtt = sample;
        }
        else
        {
            EstimatedRtt = TimeSpan.FromTicks(
                (EstimatedRtt.Value.Ticks * 7 + sample.Ticks) / 8);
        }
    }
}

internal sealed class ReceiverAckRegister
{
    private readonly AckRegister AckRegister = new();

    private readonly Queue<(uint SequenceId, byte[] Payload)> Messages = [];

    public bool MessagesAvailable => Messages.Count > 0;

    public bool Receive(uint sequenceId, byte[] payload)
    {
        bool isNew = AckRegister.Receive(sequenceId);

        if(isNew)
        {
            Messages.Enqueue((sequenceId, payload));
        }

        return isNew;
    }

    public (uint sequenceId, byte[] payload)? DequeueMessage()
    {
        if (!MessagesAvailable)
        {
            return null;;
        }

        return Messages.Dequeue();
    }

    public void BuildAcknowledgements(List<IAcknowledgement> acknowledgements)
    {
        AckRegister.BuildAcknowledgements(acknowledgements);
    }


}