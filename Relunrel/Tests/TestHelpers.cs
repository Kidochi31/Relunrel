namespace Relunrel.Tests;

internal static class TestHelpers
{
    public static void Assert(bool condition, string message)
    {
        if(!condition)
        {
            Console.WriteLine($"FAIL: {message}");
        }
    }
}