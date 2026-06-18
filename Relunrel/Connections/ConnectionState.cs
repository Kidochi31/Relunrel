namespace Relunrel.Connections;

public enum ConnectionState
{
    Disconnected,
    ActiveConnect,
    PassiveConnectAttemptComplete,
    Connected,
    FinWaitActive,
    FinWaitPassive,
    TimeWait
}

