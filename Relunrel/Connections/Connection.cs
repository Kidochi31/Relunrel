using System.Net;
using Relunrel.Packets;

namespace Relunrel.Connections;

internal sealed class Connection
{

    public ConnectionState State {get; private set;}
    public uint SessionId {get; private set;}
    public uint ConnectionToken {get; private set;}
    public readonly IPEndPoint RemoteEndPoint;
    private readonly Queue<Packet> OutgoingPackets = new();
    private DateTime LastSendTime;
    private DateTime LastReceiveTime;
    private DateTime StateEnterTime;
    private int TimeoutCount;


    public bool PacketsAvailable => OutgoingPackets.Count > 0;

    public static Connection CreatePassiveConnection(uint sessionId, uint connectionToken, IPEndPoint remoteEndPoint, DateTime time)
    {
        Connection connection = new Connection(sessionId, connectionToken, remoteEndPoint, ConnectionState.PassiveConnectAttemptComplete, time);
        connection.QueuePacket(PacketType.RespondConnect, time);
        return connection;
    }

    public static Connection CreateActiveConnection(uint sessionId, IPEndPoint remoteEndPoint, DateTime time)
    {
        Connection connection = new Connection(sessionId, 0, remoteEndPoint, ConnectionState.ActiveConnect, time);
        connection.QueuePacket(PacketType.RequestConnect, time);
        return connection;
    }

    public bool Disconnect(DateTime time)
    {
        if(State != ConnectionState.Connected)
        {
            return false;
        }

        QueuePacket(PacketType.RequestFin, time);

        SetState(ConnectionState.FinWaitActive, time);

        return true;
    }

    private Connection(uint sessionId, uint connectionToken, IPEndPoint remoteEndPoint, ConnectionState initialState, DateTime time)
    {
        SessionId = sessionId;
        ConnectionToken = connectionToken;
        RemoteEndPoint = remoteEndPoint;
        SetState(initialState, time);
    }

    private void QueuePacket(PacketType type, DateTime time)
    {
        OutgoingPackets.Enqueue(
            new Packet
            {
                Header = new PacketHeader
                {
                    ProtocolVersion = Constants.ProtocolVersion,
                    SessionId = SessionId,
                    ConnectionToken = ConnectionToken,
                    PacketType = type
                }
            });
        LastSendTime = time;
    }

    private void SetState(ConnectionState state, DateTime time)
    {
        State = state;
        StateEnterTime = time;
        TimeoutCount = 0;
    }

    public void HandlePacket(Packet packet, DateTime time)
    {
        LastReceiveTime = time;
        switch(State)
        {
            case ConnectionState.ActiveConnect:
                HandleActiveConnect(packet, time);
                break;

            case ConnectionState.PassiveConnectAttemptComplete:
                HandlePassiveConnectAttemptComplete(packet, time);
                break;

            case ConnectionState.Connected:
                HandleConnected(packet, time);
                break;

            case ConnectionState.FinWaitActive:
                HandleFinWaitActive(packet, time);
                break;

            case ConnectionState.FinWaitPassive:
                HandleFinWaitPassive(packet, time);
                break;

            case ConnectionState.TimeWait:
                HandleTimeWait(packet, time);
                break;
        }
    }

    private void HandleActiveConnect(Packet packet, DateTime time)
    {
        if(packet.Header.PacketType == PacketType.RespondConnect)
        {
            ConnectionToken =
                packet.Header.ConnectionToken;

            QueuePacket(PacketType.CompleteConnect, time);

            SetState(ConnectionState.Connected, time);
        }
        else if(packet.Header.PacketType == PacketType.Reset)
        {
            SetState(ConnectionState.Disconnected, time);
        }
    }

    private void HandlePassiveConnectAttemptComplete(Packet packet, DateTime time)
    {
        if(packet.Header.PacketType == PacketType.CompleteConnect)
        {
            SetState(ConnectionState.Connected, time);
        }
        else if(packet.Header.PacketType == PacketType.Reset)
        {
            SetState(ConnectionState.Disconnected, time);
        }
    }

    private void HandleConnected(Packet packet, DateTime time)
    {
        switch(packet.Header.PacketType)
        {
            case PacketType.RespondConnect:
                QueuePacket(PacketType.CompleteConnect, time);
                break;

            case PacketType.RequestFin:
                QueuePacket(PacketType.RespondFin, time);
                SetState(ConnectionState.FinWaitPassive, time);
                break;

            case PacketType.Reset:
                SetState(ConnectionState.Disconnected, time);
                break;
        }
    }

    private void HandleFinWaitActive(Packet packet, DateTime time)
    {
        switch(packet.Header.PacketType)
        {
            case PacketType.RespondFin:
                QueuePacket(PacketType.CompleteFin, time);
                SetState(ConnectionState.TimeWait, time);
                break;

            case PacketType.Reset:
                SetState(ConnectionState.Disconnected, time);
                break;
        }
    }

    private void HandleFinWaitPassive(Packet packet, DateTime time)
    {
        switch(packet.Header.PacketType)
        {
            case PacketType.CompleteFin:
                SetState(ConnectionState.Disconnected,  time);
                break;

            case PacketType.Reset:
                SetState(ConnectionState.Disconnected, time);
                break;
        }
    }

    private void HandleTimeWait(Packet packet, DateTime time)
    {
        switch(packet.Header.PacketType)
        {
            case PacketType.RespondFin:
                QueuePacket(PacketType.CompleteFin, time);
                break;

            case PacketType.Reset:
                SetState(ConnectionState.Disconnected, time);
                break;
        }
    }

    public void Tick(DateTime time)
    {
        switch(State)
        {
            case ConnectionState.ActiveConnect:
                TickActiveConnect(time);
                break;

            case ConnectionState.PassiveConnectAttemptComplete:
                TickPassiveConnectAttemptComplete(time);
                break;

            case ConnectionState.Connected:
                TickConnected(time);
                break;

            case ConnectionState.FinWaitActive:
                TickFinWaitActive(time);
                break;

            case ConnectionState.FinWaitPassive:
                TickFinWaitPassive(time);
                break;

            case ConnectionState.TimeWait:
                TickTimeWait(time);
                break;
        }
    }

    private void TickActiveConnect(DateTime time)
    {
        if(time - LastSendTime < Constants.ConnectTimeout)
        {
            return;
        }

        TimeoutCount++;

        if(TimeoutCount > Constants.MaxConnectRetries)
        {
            QueuePacket(PacketType.Reset, time);
            SetState(ConnectionState.Disconnected, time);
            return;
        }

        QueuePacket(PacketType.RequestConnect, time);
    }

    private void TickPassiveConnectAttemptComplete(DateTime time)
    {
        if(time - LastSendTime < Constants.ConnectTimeout)
        {
            return;
        }

        TimeoutCount++;

        if(TimeoutCount > Constants.MaxConnectRetries)
        {
            QueuePacket(PacketType.Reset, time);
            SetState(ConnectionState.Disconnected, time);
            return;
        }

        QueuePacket(PacketType.RespondConnect, time);
    }

    private void TickConnected(DateTime time)
    {
        if(time - LastReceiveTime >= Constants.ConnectionTimeout)
        {
            QueuePacket(PacketType.Reset, time);
            SetState(ConnectionState.Disconnected, time);
            return;
        }

        if(time - LastSendTime >= Constants.HeartbeatInterval)
        {
            QueuePacket(PacketType.Heartbeat, time);
        }
    }

    private void TickFinWaitActive(DateTime time)
    {
        if(time - LastSendTime < Constants.ConnectTimeout)
        {
            return;
        }

        TimeoutCount++;

        if(TimeoutCount > Constants.MaxConnectRetries)
        {
            QueuePacket(PacketType.Reset, time);
            SetState(ConnectionState.Disconnected, time);
            return;
        }

        QueuePacket(PacketType.RequestFin, time);
    }

    private void TickFinWaitPassive(DateTime time)
    {
        if(time - LastSendTime < Constants.ConnectTimeout)
        {
            return;
        }

        TimeoutCount++;

        if(TimeoutCount > Constants.MaxConnectRetries)
        {
            QueuePacket(PacketType.Reset, time);
            SetState(ConnectionState.Disconnected, time);
            return;
        }

        QueuePacket(PacketType.RespondFin, time);
    }

    private void TickTimeWait(DateTime time)
    {
        if(time - StateEnterTime >= Constants.TimeWaitTimeout)
        {
            SetState(ConnectionState.Disconnected, time);
        }
    }

    public Packet? DequeuePacket()
    {
        if(OutgoingPackets.Count == 0)
        {
            return null;
        }
        return OutgoingPackets.Dequeue();
    }

}