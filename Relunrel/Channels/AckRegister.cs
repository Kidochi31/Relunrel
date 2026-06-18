namespace Relunrel.Channels;


internal interface IAcknowledgement {}
internal readonly record struct AckMask(uint RelativeSequenceId, ulong Mask) : IAcknowledgement;

internal readonly record struct AckContiguous(uint SequenceId) : IAcknowledgement;

internal sealed class AckRegister
{
    internal uint HighestContiguousSequenceId = uint.MaxValue;

    internal readonly HashSet<uint> SparseSequenceIds = [];

    internal readonly SortedSet<uint> PendingAcknowledgements = [];

    public bool Receive(uint sequenceId)
    {
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