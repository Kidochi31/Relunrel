namespace Relunrel.Connections;

public static class Constants
{
    public const int MaxMessageSize = 1024;
    public const byte ProtocolVersion = 0;

    public static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(1);
    public const int MaxConnectRetries = 5;

    public static readonly TimeSpan HeartbeatInterval = TimeSpan.FromSeconds(5);
    public static readonly TimeSpan ConnectionTimeout = TimeSpan.FromSeconds(20);

    public static readonly TimeSpan TimeWaitTimeout = TimeSpan.FromSeconds(2);
}