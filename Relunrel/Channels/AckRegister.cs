namespace Relunrel.Channels;


internal interface IAcknowledgement {}
internal readonly record struct AckMask : IAcknowledgement
{
    public readonly uint RelativeSequenceId;

    public readonly ulong Mask;

    public AckMask(uint RelativeSequenceId, ulong Mask)
    {
        this.RelativeSequenceId = RelativeSequenceId;
        this.Mask = Mask;
    }
}

internal readonly record struct AckContiguous : IAcknowledgement
{
    public readonly uint SequenceId;

    public AckContiguous(uint SequenceId)
    {
        this.SequenceId = SequenceId;
    }
}

internal sealed class AckRegister
{
    internal uint HighestContiguousSequenceId = uint.MaxValue;

    public uint ReceiveWindowSize { get; set; } = 4096;

    internal readonly HashSet<uint> SparseSequenceIds = [];

    internal readonly SortedSet<uint> PendingAcknowledgements = [];
    
    private bool IsWithinReceiveWindow(uint sequenceId)
    {
        uint oldestAllowedSequenceId = HighestContiguousSequenceId - ReceiveWindowSize;
        uint newestAllowedSequenceId = HighestContiguousSequenceId + ReceiveWindowSize;

        return !SequenceNumber.IsOlder(sequenceId, oldestAllowedSequenceId)
            && !SequenceNumber.IsNewer(sequenceId, newestAllowedSequenceId);
    }

    public bool Receive(uint sequenceId)
    {

        if(!IsWithinReceiveWindow(sequenceId))
        {
            return false;
        }

        if(Contains(sequenceId))
        {
            PendingAcknowledgements.Add(sequenceId);
            return false;
        }

        PendingAcknowledgements.Add(sequenceId);

        unchecked{

            if(sequenceId == HighestContiguousSequenceId + 1)
            {
                HighestContiguousSequenceId++;

                while(SparseSequenceIds.Remove(HighestContiguousSequenceId + 1))
                {
                    HighestContiguousSequenceId++;
                }
            }
            else if(SequenceNumber.IsNewer(sequenceId, HighestContiguousSequenceId))
            {
                SparseSequenceIds.Add(sequenceId);
            }
        }

        return true;
    }

    public bool Contains(uint sequenceId)
    {
        if(!SequenceNumber.IsNewer(sequenceId, HighestContiguousSequenceId))
        {
            return true;
        }

        return SparseSequenceIds.Contains(sequenceId);
    }

    public void BuildAcknowledgements(List<IAcknowledgement> acknowledgements)
    {
        while(PendingAcknowledgements.Count > 0)
        {
            uint highest = PendingAcknowledgements.Max;

            if(!SequenceNumber.IsNewer(highest, HighestContiguousSequenceId))
            {
                acknowledgements.Add(new AckContiguous(HighestContiguousSequenceId));

                PendingAcknowledgements.RemoveWhere(
                    sequenceId => !SequenceNumber.IsNewer(sequenceId, HighestContiguousSequenceId));

                continue;
            }

            ulong mask = 0;
            unchecked{

                for(int i = 0; i < 64; i++)
                {
                    uint sequenceId = highest - (uint)i;
                    PendingAcknowledgements.Remove(sequenceId);

                    if (Contains(sequenceId))
                    {
                        mask |= 1UL << i;
                    }
                }
            }

            acknowledgements.Add(new AckMask(highest, mask));
        }
    }
}