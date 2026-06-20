using System.Net;
using Relunrel.Packets;
using Relunrel.Channels;

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

    private uint NextUnreliableOrderedSequenceId;

    private readonly ReliableSender ReliableSender = new();

    private readonly ReliableReceiver ReliableReceiver = new();

    private readonly ReliableOrderedReceiver ReliableOrderedReceiver = new();

    private readonly UnreliableOrderedReceiver UnreliableOrderedReceiver = new();

    private readonly Queue<byte[]> ReceivedReliableUnorderedMessages = [];

    private readonly Queue<byte[]> ReceivedReliableOrderedMessages = [];

    private readonly Queue<byte[]> ReceivedUnreliableUnorderedMessages = [];

    private readonly Queue<byte[]> ReceivedUnreliableOrderedMessages = [];

    private readonly Queue<Record> PendingRecords = [];

    public bool PacketsAvailable => OutgoingPackets.Count > 0;

    public bool ReliableUnorderedMessagesAvailable =>
    ReceivedReliableUnorderedMessages.Count > 0;

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

    public byte[]? DequeueReliableUnorderedMessage()
    {
        if(ReceivedReliableUnorderedMessages.Count == 0)
        {
            return null;
        }

        return ReceivedReliableUnorderedMessages.Dequeue();
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
        if(packet.Header.SessionId != SessionId)
        {
            return;
        }
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
        if(packet.Header.SessionId != SessionId || packet.Header.ConnectionToken != ConnectionToken)
        {
            return;
        }
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
        if(packet.Header.SessionId != SessionId || packet.Header.ConnectionToken != ConnectionToken)
        {
            return;
        }
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
            
            case PacketType.Message:
                HandleMessagePacket(packet, time);
                break;
        }
    }

    private void HandleMessagePacket(Packet packet, DateTime time)
    {
        if(packet.Header.SessionId != SessionId || packet.Header.ConnectionToken != ConnectionToken)
        {
            return;
        }
        if(packet.Body is not MessagePacket messagePacket)
        {
            return;
        }

        foreach(Record record in messagePacket.Records)
        {
            switch(record)
            {
                case ReliableUnorderedRecord reliable:
                    HandleReliableUnordered(reliable);
                    break;
                case ReliableOrderedRecord reliableOrdered:
                    {
                        if(ReliableReceiver.Receive(
                            reliableOrdered.SequenceId,
                            reliableOrdered.Payload))
                        {
                            ReliableOrderedReceiver.Receive(
                                reliableOrdered.SequenceId,
                                reliableOrdered.Payload);

                            while(ReliableOrderedReceiver.MessagesAvailable)
                            {
                                (uint SequenceId, byte[] Payload)? message =
                                    ReliableOrderedReceiver.DequeueMessage();

                                if(message is not null)
                                {
                                    ReceivedReliableOrderedMessages.Enqueue(
                                        message.Value.Payload);
                                }
                            }
                        }

                        break;
                    }
                case UnreliableOrderedRecord unreliableOrdered:
                    {
                        if(UnreliableOrderedReceiver.Receive(
                            unreliableOrdered.SequenceId,
                            unreliableOrdered.Payload))
                        {
                            while(UnreliableOrderedReceiver.MessagesAvailable)
                            {
                                (uint SequenceId, byte[] Payload)? message =
                                    UnreliableOrderedReceiver.DequeueMessage();

                                if(message is not null)
                                {
                                    ReceivedUnreliableOrderedMessages.Enqueue(
                                        message.Value.Payload);
                                }
                            }
                        }

                        break;
                    }
                case UnreliableUnorderedRecord unreliableUnordered:
                    {
                        ReceivedUnreliableUnorderedMessages.Enqueue(
                            unreliableUnordered.Payload);

                        break;
                    }
                case AckContiguousRecord contiguous:
                    ReliableSender.HandleAcknowledgementContiguous(
                        contiguous.AcknowledgedSequenceId,
                        time);
                    break;

                case AckMaskRecord mask:
                    ReliableSender.HandleAcknowledgementMask(
                        mask.RelativeSequenceId,
                        mask.AckBitfield,
                        time);
                    break;
            }
        }
    }

    private void HandleReliableUnordered(ReliableUnorderedRecord record)
    {
        if(ReliableReceiver.Receive(
            record.SequenceId,
            record.Payload))
        {
            ReceivedReliableUnorderedMessages.Enqueue(
                record.Payload);
        }
    }

    private void HandleFinWaitActive(Packet packet, DateTime time)
    {
        if(packet.Header.SessionId != SessionId || packet.Header.ConnectionToken != ConnectionToken)
        {
            return;
        }
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
        if(packet.Header.SessionId != SessionId || packet.Header.ConnectionToken != ConnectionToken)
        {
            return;
        }
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
        if(packet.Header.SessionId != SessionId || packet.Header.ConnectionToken != ConnectionToken)
        {
            return;
        }
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
        QueueAcknowledgements();
        BuildMessagePackets(time);
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
        ReliableSender.Tick(time);

        while(ReliableSender.RetransmissionsAvailable)
        {
            PendingMessage? message = ReliableSender.DequeueRetransmission();

            if(message is null)
            {
                continue;
            }

            ReliableUnorderedRecord? record =
                ReliableUnorderedRecord.Create(
                    message.SequenceId,
                    message.Payload);

            if(record is not null)
            {
                QueueRecord(record);
            }
        }

        if(ReliableSender.ResetRequired)
        {
            QueuePacket(PacketType.Reset, time);
            SetState(ConnectionState.Disconnected, time);
            return;
        }
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

    private void QueueRecord(Record record)
    {
        PendingRecords.Enqueue(record);
    }

    public bool SendReliableUnordered(byte[] payload, DateTime time)
    {
        if(State != ConnectionState.Connected)
        {
            return false;
        }

        uint sequenceId = ReliableSender.Send(payload, time);
        
        ReliableUnorderedRecord? record = ReliableUnorderedRecord.Create(sequenceId, payload);
        if(record is null)
        {
            return false;
        }
        QueueRecord(record);

        return true;
    }

    public bool SendReliableOrdered(byte[] payload, DateTime time)
    {
        if(State != ConnectionState.Connected)
        {
            return false;
        }

        uint sequenceId = ReliableSender.Send(payload, time);

        ReliableOrderedRecord? record =
            ReliableOrderedRecord.Create(
                sequenceId,
                payload);

        if(record is null)
        {
            return false;
        }

        QueueRecord(record);

        return true;
    }

    public bool SendUnreliableUnordered(byte[] payload)
    {
        if(State != ConnectionState.Connected)
        {
            return false;
        }

        UnreliableUnorderedRecord? record =
            UnreliableUnorderedRecord.Create(
                payload);

        if(record is null)
        {
            return false;
        }

        QueueRecord(record);

        return true;
    }

    private void BuildMessagePackets(DateTime time)
    {
        while(PendingRecords.Count > 0)
        {
            MessagePacket packet = new();

            int size = PacketHeader.HeaderSize;

            while(PendingRecords.Count > 0)
            {
                Record record = PendingRecords.Peek();

                int recordSize = record.GetSerializedSize();

                if(size + recordSize > Packet.MaximumPacketSize)
                {
                    break;
                }

                packet.Records.Add(record);

                PendingRecords.Dequeue();

                size += recordSize;
            }

            OutgoingPackets.Enqueue(
                new Packet
                {
                    Header = new PacketHeader
                    {
                        ProtocolVersion=Constants.ProtocolVersion,
                        SessionId=SessionId,
                        ConnectionToken=ConnectionToken,
                        PacketType=PacketType.Message
                    },
                    Body = packet
                });
        }
    }

    public bool SendUnreliableOrdered(byte[] payload)
    {
        if(State != ConnectionState.Connected)
        {
            return false;
        }

        UnreliableOrderedRecord? record =
            UnreliableOrderedRecord.Create(
                NextUnreliableOrderedSequenceId++,
                payload);

        if(record is null)
        {
            return false;
        }

        QueueRecord(record);

        return true;
    }

    public bool UnreliableOrderedMessagesAvailable =>
    ReceivedUnreliableOrderedMessages.Count > 0;

    public byte[]? DequeueUnreliableOrderedMessage()
    {
        if(ReceivedUnreliableOrderedMessages.Count == 0)
        {
            return null;
        }

        return ReceivedUnreliableOrderedMessages.Dequeue();
    }

    public bool UnreliableUnorderedMessagesAvailable =>
    ReceivedUnreliableUnorderedMessages.Count > 0;

    public byte[]? DequeueUnreliableUnorderedMessage()
    {
        if(ReceivedUnreliableUnorderedMessages.Count == 0)
        {
            return null;
        }

        return ReceivedUnreliableUnorderedMessages.Dequeue();
    }

    public bool ReliableOrderedMessagesAvailable => ReceivedReliableOrderedMessages.Count > 0;

    public byte[]? DequeueReliableOrderedMessage()
    {
        if(ReceivedReliableOrderedMessages.Count == 0)
        {
            return null;
        }

        return ReceivedReliableOrderedMessages.Dequeue();
    }

    private void QueueAcknowledgements()
    {
        List<IAcknowledgement> acknowledgements = [];

        ReliableReceiver.BuildAcknowledgements(acknowledgements);

        foreach(IAcknowledgement acknowledgement in acknowledgements)
        {
            switch(acknowledgement)
            {
                case AckContiguous contiguous:
                    QueueRecord(
                        new AckContiguousRecord(
                            RecordType.AckContiguousReliableUnordered,contiguous.SequenceId));
                    break;

                case AckMask mask:
                    QueueRecord(
                        new AckMaskRecord(
                            RecordType.AckMaskReliableUnordered,mask.RelativeSequenceId,mask.Mask));
                    break;
            }
        }
    }
}