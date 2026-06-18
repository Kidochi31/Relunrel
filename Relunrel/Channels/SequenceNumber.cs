namespace Relunrel.Channels;

internal static class SequenceNumber
{
    public static bool IsNewer(uint a, uint b)
    {
        uint diff = a - b;

        return diff > 0 && diff < 0x80000000u;
    }

    public static bool IsOlder(uint a, uint b)
    {
        return IsNewer(b, a);
    }

    public static int Compare(uint a, uint b)
    {
        if(a == b)
        {
            return 0;
        }

        return IsNewer(a, b) ? 1 : -1;
    }
}