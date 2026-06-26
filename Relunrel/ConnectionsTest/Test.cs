using System.Net;
using Relunrel.Connections;
using Relunrel.Channels;
using static Relunrel.Tests.TestHelpers;
using Relunrel.Packets;

namespace Relunrel.ConnectionsTest;

public class Test{

    public static void RunTest()
    {
        TestConnectionHandshake();
        TestDisconnect();
        TestActiveConnectRetransmit();
        TestActiveConnectFailure();
        TestPassiveConnectFailure();
        TestHeartbeat();
        TestConnectionTimeout();
        TestTimeWaitExpiry();
        TestFinWaitActiveRetransmit();
        TestFinWaitActiveFailure();
        TestFinWaitPassiveRetransmit();
        TestFinWaitPassiveFailure();
        TestTimeWaitDuplicateRespondFin();
        TestHeartbeatPreventsTimeout();
        TestHeartbeatResetsTimeout();
        TestAckRegisterContiguous();
        TestAckRegisterSparse();
        TestAckRegisterDuplicate();
        TestAckRegisterGapClosure();
        TestAckRegisterPendingCleared();
        TestReliableSendReceive();
        TestReliableDuplicateMessage();
        TestReliableContiguousAcknowledgement();
        TestReliableMaskAcknowledgement();
        TestDuplicateGeneratesAcknowledgement();
        TestReliableSparseAcknowledgement();
        TestReliableRetransmission();
        TestReliableAcknowledgedNotRetransmitted();
        TestReliableRetryLimit();
        TestReliableRttMeasurement();
        TestReliableOrderedInOrder();
        TestReliableOrderedOutOfOrder();
        TestReliableOrderedGapClosure();
        TestUnreliableOrdered();
        TestReliableOrderedDuplicate();
        TestSendReliableUnordered();
        TestReliableUnorderedDelivery();
        TestReliableAckGeneration();
        TestReliableAcknowledgementFlow();
        TestReliableOrderedReordering();
        TestUnreliableOrderedDropsOld();
        TestReliableOrderedDelivery();
        TestAckRegisterReceiveWindow();
        TestReliableUnorderedStress();
        TestReliableOrderedStress();
        TestSequenceWraparound();
        TestReliableLossDuplicationStress();
    }

    public static void TestConnectionHandshake()
    {
        DateTime time = DateTime.UtcNow;

        IPEndPoint clientEndPoint = new(IPAddress.Parse("127.0.0.1"), 10000);
        IPEndPoint serverEndPoint = new(IPAddress.Parse("127.0.0.1"), 10001);

        Listener listener = new();

        Connection client = Connection.CreateActiveConnection(12345, serverEndPoint, time);

        Assert(client.State == ConnectionState.ActiveConnect, "Client state is ActiveConnect");
        Assert(client.PacketsAvailable, "Client queued RequestConnect");

        Packet? requestPacket = client.DequeuePacket();

        Assert(requestPacket != null, "Client produced RequestConnect packet");
        Assert(requestPacket!.Header.PacketType == PacketType.RequestConnect, "Packet type is RequestConnect");

        Connection? server = listener.HandlePacket(requestPacket, clientEndPoint, time);

        Assert(server != null, "Listener accepted connection");
        Assert(server!.State == ConnectionState.PassiveConnectAttemptComplete, "Server state is PassiveConnectAttemptComplete");
        Assert(server.PacketsAvailable, "Server queued RespondConnect");

        Packet? respondPacket = server.DequeuePacket();

        Assert(respondPacket != null, "Server produced RespondConnect packet");
        Assert(respondPacket!.Header.PacketType == PacketType.RespondConnect, "Packet type is RespondConnect");

        client.HandlePacket(respondPacket, time);

        Assert(client.State == ConnectionState.Connected, "Client entered Connected");
        Assert(client.PacketsAvailable, "Client queued CompleteConnect");

        Packet? completePacket = client.DequeuePacket();

        Assert(completePacket != null, "Client produced CompleteConnect packet");
        Assert(completePacket!.Header.PacketType == PacketType.CompleteConnect, "Packet type is CompleteConnect");

        server.HandlePacket(completePacket, time);

        Assert(server.State == ConnectionState.Connected, "Server entered Connected");

        Assert(client.SessionId == server.SessionId, "Session IDs match");
        Assert(client.ConnectionToken == server.ConnectionToken, "Connection tokens match");

        Assert(!client.PacketsAvailable, "Client queue empty");
        Assert(!server.PacketsAvailable, "Server queue empty");

        Console.WriteLine("PASS: Connection handshake");
    }

    private static void TestDisconnect()
    {
        DateTime time = DateTime.UtcNow;

        IPEndPoint clientEndPoint = new(IPAddress.Parse("127.0.0.1"), 10000);
        IPEndPoint serverEndPoint = new(IPAddress.Parse("127.0.0.1"), 10001);

        Listener listener = new();

        Connection client = Connection.CreateActiveConnection(12345, serverEndPoint, time);

        Packet? requestPacket = client.DequeuePacket();

        Assert(requestPacket != null, "RequestConnect packet exists");

        Connection? server = listener.HandlePacket(requestPacket!, clientEndPoint, time);

        Assert(server != null, "Listener created connection");

        Packet? respondPacket = server!.DequeuePacket();

        Assert(respondPacket != null, "RespondConnect packet exists");

        client.HandlePacket(respondPacket!, time);

        Packet? completePacket = client.DequeuePacket();

        Assert(completePacket != null, "CompleteConnect packet exists");

        server.HandlePacket(completePacket!, time);

        Assert(client.State == ConnectionState.Connected, "Client connected");
        Assert(server.State == ConnectionState.Connected, "Server connected");

        Assert(client.Disconnect(time), "Disconnect started");

        Assert(client.State == ConnectionState.FinWaitActive, "Client entered FinWaitActive");

        Packet? requestFinPacket = client.DequeuePacket();

        Assert(requestFinPacket != null, "RequestFin packet exists");
        Assert(requestFinPacket!.Header.PacketType == PacketType.RequestFin, "RequestFin packet type");

        server.HandlePacket(requestFinPacket, time);

        Assert(server.State == ConnectionState.FinWaitPassive, "Server entered FinWaitPassive");

        Packet? respondFinPacket = server.DequeuePacket();

        Assert(respondFinPacket != null, "RespondFin packet exists");
        Assert(respondFinPacket!.Header.PacketType == PacketType.RespondFin, "RespondFin packet type");

        client.HandlePacket(respondFinPacket, time);

        Assert(client.State == ConnectionState.TimeWait, "Client entered TimeWait");

        Packet? completeFinPacket = client.DequeuePacket();

        Assert(completeFinPacket != null, "CompleteFin packet exists");
        Assert(completeFinPacket!.Header.PacketType == PacketType.CompleteFin, "CompleteFin packet type");

        server.HandlePacket(completeFinPacket, time);

        Assert(server.State == ConnectionState.Disconnected, "Server disconnected");

        Assert(!client.PacketsAvailable, "Client queue empty");
        Assert(!server.PacketsAvailable, "Server queue empty");

        Console.WriteLine("PASS: Disconnect");
    }   

    private static DateTime Advance(DateTime time, TimeSpan duration)
    {
        return time + duration;
    }

    private static void TestActiveConnectRetransmit()
    {
        DateTime time = DateTime.UtcNow;

        Connection connection = Connection.CreateActiveConnection(12345, new IPEndPoint(IPAddress.Loopback, 1234), time);

        Assert(connection.PacketsAvailable, "Initial RequestConnect queued");

        connection.DequeuePacket();

        time = Advance(time, Constants.ConnectTimeout);

        connection.Tick(time);

        Assert(connection.State == ConnectionState.ActiveConnect, "Still ActiveConnect");
        Assert(connection.PacketsAvailable, "Retransmitted RequestConnect");

        Packet? packet = connection.DequeuePacket();

        Assert(packet != null, "Packet exists");
        Assert(packet!.Header.PacketType == PacketType.RequestConnect, "Packet is RequestConnect");

        Console.WriteLine("PASS: Active connect retransmit");
    }

    private static void TestActiveConnectFailure()
    {
        DateTime time = DateTime.UtcNow;

        Connection connection = Connection.CreateActiveConnection(12345, new IPEndPoint(IPAddress.Loopback, 1234), time);

        connection.DequeuePacket();

        for(int i = 0; i <= Constants.MaxConnectRetries; i++)
        {
            time = Advance(time, Constants.ConnectTimeout);
            connection.Tick(time);

            while(connection.PacketsAvailable)
            {
                connection.DequeuePacket();
            }
        }

        Assert(connection.State == ConnectionState.Disconnected, "Disconnected after retries");

        Console.WriteLine("PASS: Active connect failure");
    }

    private static void TestPassiveConnectFailure()
    {
        DateTime time = DateTime.UtcNow;

        Listener listener = new();

        Packet request = new()
        {
            Header = new PacketHeader
            {
                ProtocolVersion = Constants.ProtocolVersion,
                SessionId = 12345,
                ConnectionToken = 0,
                PacketType = PacketType.RequestConnect
            }
        };

        Connection? connection = listener.HandlePacket(request, new IPEndPoint(IPAddress.Loopback, 1234), time);

        Assert(connection != null, "Connection created");

        while(connection!.PacketsAvailable)
        {
            connection.DequeuePacket();
        }

        for(int i = 0; i <= Constants.MaxConnectRetries; i++)
        {
            time = Advance(time, Constants.ConnectTimeout);
            connection.Tick(time);

            while(connection.PacketsAvailable)
            {
                connection.DequeuePacket();
            }
        }

        Assert(connection.State == ConnectionState.Disconnected, "Disconnected after retries");

        Console.WriteLine("PASS: Passive connect failure");
    }

    private static void TestHeartbeat()
    {
        DateTime time = DateTime.UtcNow;

        Connection client;
        Connection server;

        CreateConnectedPair(out client, out server, time);

        time = Advance(time, Constants.HeartbeatInterval);

        client.Tick(time);

        Assert(client.PacketsAvailable, "Heartbeat queued");

        Packet? packet = client.DequeuePacket();

        Assert(packet != null, "Heartbeat packet exists");
        Assert(packet!.Header.PacketType == PacketType.Heartbeat, "Packet is Heartbeat");

        Console.WriteLine("PASS: Heartbeat");
    }

    private static void TestConnectionTimeout()
    {
        DateTime time = DateTime.UtcNow;

        Connection client;
        Connection server;

        CreateConnectedPair(out client, out server, time);

        time = Advance(time, Constants.ConnectionTimeout);

        client.Tick(time);

        Assert(client.State == ConnectionState.Disconnected, "Disconnected after timeout");

        Assert(client.PacketsAvailable, "Reset queued");

        bool foundReset = false;

        while(client.PacketsAvailable)
        {
            Packet? packet = client.DequeuePacket();
            Assert(packet != null, "Reset packet exists");

            if(packet!.Header.PacketType == PacketType.Reset)
            {
                foundReset = true;
            }
        }

        Assert(foundReset, "Reset queued");

        Console.WriteLine("PASS: Connection timeout");
    }

    private static void TestTimeWaitExpiry()
    {
        DateTime time = DateTime.UtcNow;

        Connection client;
        Connection server;

        CreateConnectedPair(out client, out server, time);

        Assert(client.Disconnect(time), "Disconnect started");

        Packet requestFin = client.DequeuePacket()!;
        server.HandlePacket(requestFin, time);

        Packet respondFin = server.DequeuePacket()!;
        client.HandlePacket(respondFin, time);

        Assert(client.State == ConnectionState.TimeWait, "Client entered TimeWait");

        time = Advance(time, Constants.TimeWaitTimeout);

        client.Tick(time);

        Assert(client.State == ConnectionState.Disconnected, "TimeWait expired");

        Console.WriteLine("PASS: TimeWait expiry");
    }

    private static void CreateConnectedPair(out Connection client, out Connection server, DateTime time)
    {
        IPEndPoint clientEndPoint = new(IPAddress.Loopback, 10000);
        IPEndPoint serverEndPoint = new(IPAddress.Loopback, 10001);

        Listener listener = new();

        client = Connection.CreateActiveConnection(12345, serverEndPoint, time);

        Packet request = client.DequeuePacket()!;

        Connection? passive = listener.HandlePacket(request, clientEndPoint, time);

        Assert(passive != null, "Passive connection created");

        server = passive!;

        Packet respond = server.DequeuePacket()!;

        client.HandlePacket(respond, time);

        Packet complete = client.DequeuePacket()!;

        server.HandlePacket(complete, time);

        Assert(client.State == ConnectionState.Connected, "Client connected");
        Assert(server.State == ConnectionState.Connected, "Server connected");
    }

    private static void TestFinWaitActiveRetransmit()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection client, out Connection server, time);

        Assert(client.Disconnect(time), "Disconnect started");

        Assert(client.PacketsAvailable, "RequestFin queued");

        client.DequeuePacket();

        time += Constants.ConnectTimeout;

        client.Tick(time);

        Assert(client.State == ConnectionState.FinWaitActive, "Still FinWaitActive");
        Assert(client.PacketsAvailable, "RequestFin retransmitted");

        Packet? packet = client.DequeuePacket();

        Assert(packet != null, "Packet exists");
        Assert(packet!.Header.PacketType == PacketType.RequestFin, "Packet is RequestFin");

        Console.WriteLine("PASS: FinWaitActive retransmit");
    }

    private static void TestFinWaitActiveFailure()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection client, out Connection server, time);

        Assert(client.Disconnect(time), "Disconnect started");

        while(client.PacketsAvailable)
        {
            client.DequeuePacket();
        }

        for(int i = 0; i <= Constants.MaxConnectRetries; i++)
        {
            time += Constants.ConnectTimeout;

            client.Tick(time);

            while(client.PacketsAvailable)
            {
                client.DequeuePacket();
            }
        }

        Assert(client.State == ConnectionState.Disconnected, "Client disconnected");

        Console.WriteLine("PASS: FinWaitActive failure");
    }

    private static void TestFinWaitPassiveRetransmit()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection client, out Connection server, time);

        Assert(client.Disconnect(time), "Disconnect started");

        Packet requestFin = client.DequeuePacket()!;

        server.HandlePacket(requestFin, time);

        Assert(server.State == ConnectionState.FinWaitPassive, "Server entered FinWaitPassive");

        server.DequeuePacket();

        time += Constants.ConnectTimeout;

        server.Tick(time);

        Assert(server.State == ConnectionState.FinWaitPassive, "Still FinWaitPassive");
        Assert(server.PacketsAvailable, "RespondFin retransmitted");

        Packet? packet = server.DequeuePacket();

        Assert(packet != null, "Packet exists");
        Assert(packet!.Header.PacketType == PacketType.RespondFin, "Packet is RespondFin");

        Console.WriteLine("PASS: FinWaitPassive retransmit");
    }

    private static void TestFinWaitPassiveFailure()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection client, out Connection server, time);

        Assert(client.Disconnect(time), "Disconnect started");

        Packet requestFin = client.DequeuePacket()!;

        server.HandlePacket(requestFin, time);

        while(server.PacketsAvailable)
        {
            server.DequeuePacket();
        }

        for(int i = 0; i <= Constants.MaxConnectRetries; i++)
        {
            time += Constants.ConnectTimeout;

            server.Tick(time);

            while(server.PacketsAvailable)
            {
                server.DequeuePacket();
            }
        }

        Assert(server.State == ConnectionState.Disconnected, "Server disconnected");

        Console.WriteLine("PASS: FinWaitPassive failure");
    }

    private static void TestTimeWaitDuplicateRespondFin()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection client, out Connection server, time);

        Assert(client.Disconnect(time), "Disconnect started");

        Packet requestFin = client.DequeuePacket()!;

        server.HandlePacket(requestFin, time);

        Packet respondFin = server.DequeuePacket()!;

        client.HandlePacket(respondFin, time);

        Assert(client.State == ConnectionState.TimeWait, "Client entered TimeWait");

        client.DequeuePacket();

        client.HandlePacket(respondFin, time);

        Assert(client.PacketsAvailable, "CompleteFin requeued");

        Packet? packet = client.DequeuePacket();

        Assert(packet != null, "Packet exists");
        Assert(packet!.Header.PacketType == PacketType.CompleteFin, "Packet is CompleteFin");

        Console.WriteLine("PASS: TimeWait duplicate RespondFin");
    }

    private static void TestHeartbeatPreventsTimeout()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection client, out Connection server, time);

        for(int i = 0; i < 10; i++)
        {
            time += TimeSpan.FromSeconds(19);

            Packet heartbeat = new()
            {
                Header = new PacketHeader
                {
                    ProtocolVersion = Constants.ProtocolVersion,
                    SessionId = client.SessionId,
                    ConnectionToken = client.ConnectionToken,
                    PacketType = PacketType.Heartbeat
                }
            };

            client.HandlePacket(heartbeat, time);

            client.Tick(time);

            Assert(client.State == ConnectionState.Connected, $"Still connected iteration {i}");
        }

        Console.WriteLine("PASS: Heartbeat prevents timeout");
    }

    private static void TestHeartbeatResetsTimeout()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection client, out Connection server, time);

        time += TimeSpan.FromSeconds(19);

        Packet heartbeat = new()
        {
            Header = new PacketHeader
            {
                ProtocolVersion = Constants.ProtocolVersion,
                SessionId = client.SessionId,
                ConnectionToken = client.ConnectionToken,
                PacketType = PacketType.Heartbeat
            }
        };

        client.HandlePacket(heartbeat, time);

        time += TimeSpan.FromSeconds(19);

        client.Tick(time);

        Assert(client.State == ConnectionState.Connected, "Still connected");

        Console.WriteLine("PASS: Heartbeat resets timeout");
    }

    private static void TestAckRegisterContiguous()
    {
        AckRegister ack = new();

        Assert(ack.Receive(0), "Receive 0");
        Assert(ack.Receive(1), "Receive 1");
        Assert(ack.Receive(2), "Receive 2");

        Assert(ack.Contains(0), "Contains 0");
        Assert(ack.Contains(1), "Contains 1");
        Assert(ack.Contains(2), "Contains 2");

        List<IAcknowledgement> acknowledgements = [];

        ack.BuildAcknowledgements(acknowledgements);

        Assert(acknowledgements.Count == 1, "One acknowledgement");
        Assert(acknowledgements[0] is AckContiguous, "Contiguous acknowledgement");

        AckContiguous contiguous = (AckContiguous)acknowledgements[0];

        Assert(contiguous.SequenceId == 2, "Highest contiguous is 2");
    }

    private static void TestAckRegisterSparse()
    {
        AckRegister ack = new();

        ack.Receive(0);
        ack.Receive(1);
        ack.Receive(2);
        ack.Receive(5);

        List<IAcknowledgement> acknowledgements = [];

        ack.BuildAcknowledgements(acknowledgements);

        Assert(acknowledgements.Count == 1, "One acknowledgement");
        Assert(acknowledgements[0] is AckMask, "Mask");
        Assert(((AckMask)acknowledgements[0]).RelativeSequenceId == 5 && ((AckMask)acknowledgements[0]).Mask == ulong.MaxValue - (1<<1) - (1<<2), "Mask");
    }

    private static void TestAckRegisterDuplicate()
    {
        AckRegister ack = new();

        Assert(ack.Receive(0), "First receive");
        Assert(!ack.Receive(0), "Duplicate rejected");
    }

    private static void TestAckRegisterGapClosure()
    {
        AckRegister ack = new();

        ack.Receive(0);
        ack.Receive(1);
        ack.Receive(2);
        ack.Receive(5);
        ack.Receive(6);

        ack.Receive(3);
        ack.Receive(4);

        Assert(ack.HighestContiguousSequenceId == 6, "Gap closed to 6");
        Assert(ack.SparseSequenceIds.Count == 0, "Sparse empty");

        ack = new();

        ack.Receive(0);
        ack.Receive(1);
        ack.Receive(2);

        ack.Receive(5);
        ack.Receive(6);

        Assert(ack.HighestContiguousSequenceId == 2, "Contiguous ends at 2");
        Assert(ack.SparseSequenceIds.Contains(5), "Contains 5");
        Assert(ack.SparseSequenceIds.Contains(6), "Contains 6");

        ack.Receive(3);
        ack.Receive(4);

        Assert(ack.HighestContiguousSequenceId == 6, "Gap closed to 6");
        Assert(ack.SparseSequenceIds.Count == 0, "Sparse empty");

        Console.WriteLine("PASS: Ack gap closure");
    }

    private static void TestAckRegisterPendingCleared()
    {
        AckRegister ack = new();

        ack.Receive(0);
        ack.Receive(1);
        ack.Receive(2);

        List<IAcknowledgement> acknowledgements = [];

        ack.BuildAcknowledgements(acknowledgements);

        Assert(ack.PendingAcknowledgements.Count == 0, "Pending cleared");
    }

    private static void TestReliableSendReceive()
    {
        DateTime time = DateTime.UtcNow;

        ReliableSender sender = new();
        ReceiverAckRegister receiver = new();

        byte[] payload = [1, 2, 3, 4];

        uint sequenceId = sender.Send(payload, time);

        Assert(sender.PendingMessageCount == 1, "Sender has pending message");

        receiver.Receive(sequenceId, payload);

        Assert(receiver.MessagesAvailable, "Receiver got message");
        (uint receivedSequenceId, byte[] receivedPayload) = ((uint, byte[]))receiver.DequeueMessage();
        Assert(receivedSequenceId == sequenceId, "Sequence IDs match");
        Assert(receivedPayload.SequenceEqual(payload), "Payloads match");

        List<IAcknowledgement> acknowledgements = [];

        receiver.BuildAcknowledgements(acknowledgements);

        foreach(IAcknowledgement acknowledgement in acknowledgements)
        {
            switch(acknowledgement)
            {
                case AckContiguous contiguous:
                    sender.HandleAcknowledgementContiguous(contiguous.SequenceId, time);
                    break;

                case AckMask mask:
                    sender.HandleAcknowledgementMask(mask.RelativeSequenceId, mask.Mask, time);
                    break;
            }
        }

        Assert(sender.PendingMessageCount == 0, "Message acknowledged");

        Console.WriteLine("PASS: Reliable send receive");
    }

    private static void TestReliableDuplicateMessage()
    {
        ReceiverAckRegister receiver = new();

        Assert(receiver.Receive(0, [1]), "First receive");
        Assert(!receiver.Receive(0, [2]), "Duplicate receive");

        Assert(receiver.MessagesAvailable, "Message available");

        (uint sequenceId, byte[] payload)? message = receiver.DequeueMessage();

        Assert(message != null, "Message exists");
        Assert(message.Value.sequenceId == 0, "Sequence id correct");
        Assert(message.Value.payload.AsSpan().SequenceEqual(new byte[]{1}), "Original payload preserved");

        Assert(!receiver.MessagesAvailable, "Only one message queued");

        Console.WriteLine("PASS: Reliable duplicate message");
    }

    private static void TestReliableContiguousAcknowledgement()
    {
        DateTime time = DateTime.UtcNow;

        ReliableSender sender = new();

        sender.Send([0], time);
        sender.Send([1], time);
        sender.Send([2], time);
        sender.Send([3], time);

        sender.HandleAcknowledgementContiguous(2, time);

        Assert(!sender.HasPendingMessage(0), "0 removed");
        Assert(!sender.HasPendingMessage(1), "1 removed");
        Assert(!sender.HasPendingMessage(2), "2 removed");

        Assert(sender.HasPendingMessage(3), "3 remains");

        Console.WriteLine("PASS: Reliable contiguous acknowledgement");
    }

    private static void TestReliableMaskAcknowledgement()
    {
        DateTime time = DateTime.UtcNow;

        ReliableSender sender = new();

        sender.Send([0], time);
        sender.Send([1], time);
        sender.Send([2], time);
        sender.Send([3], time);
        sender.Send([4], time);
        sender.Send([5], time);

        ulong mask = 0;

        mask |= 1UL << 0;
        mask |= 1UL << 2;
        mask |= 1UL << 5;

        sender.HandleAcknowledgementMask(5, mask, time);

        Assert(!sender.HasPendingMessage(5), "5 removed");
        Assert(!sender.HasPendingMessage(3), "3 removed");
        Assert(!sender.HasPendingMessage(0), "0 removed");

        Assert(sender.HasPendingMessage(1), "1 remains");
        Assert(sender.HasPendingMessage(2), "2 remains");
        Assert(sender.HasPendingMessage(4), "4 remains");

        Console.WriteLine("PASS: Reliable mask acknowledgement");
    }

    private static void TestDuplicateGeneratesAcknowledgement()
    {
        AckRegister ack = new();

        ack.Receive(0);

        List<IAcknowledgement> acknowledgements = [];

        ack.BuildAcknowledgements(acknowledgements);

        Assert(acknowledgements.Count > 0, "Initial ack generated");

        acknowledgements.Clear();

        ack.Receive(0);

        ack.BuildAcknowledgements(acknowledgements);

        Assert(acknowledgements.Count > 0, "Duplicate generates ack");

        Console.WriteLine("PASS: Duplicate generates acknowledgement");
    }

    private static void TestReliableSparseAcknowledgement()
    {
        DateTime time = DateTime.UtcNow;

        ReliableSender sender = new();
        ReceiverAckRegister receiver = new();

        sender.Send([0], time);
        sender.Send([1], time);
        sender.Send([2], time);
        sender.Send([3], time);
        sender.Send([4], time);
        sender.Send([5], time);

        receiver.Receive(0, [0]);
        receiver.Receive(1, [1]);
        receiver.Receive(3, [3]);
        receiver.Receive(5, [5]);

        List<IAcknowledgement> acknowledgements = [];

        receiver.BuildAcknowledgements(acknowledgements);

        foreach(IAcknowledgement acknowledgement in acknowledgements)
        {
            switch(acknowledgement)
            {
                case AckContiguous contiguous:
                    sender.HandleAcknowledgementContiguous(contiguous.SequenceId, time);
                    break;

                case AckMask mask:
                    sender.HandleAcknowledgementMask(mask.RelativeSequenceId, mask.Mask, time);
                    break;
            }
        }

        Assert(!sender.HasPendingMessage(0), "0 acknowledged");
        Assert(!sender.HasPendingMessage(1), "1 acknowledged");

        Assert(sender.HasPendingMessage(2), "2 still pending");
        Assert(sender.HasPendingMessage(4), "4 still pending");

        Console.WriteLine("PASS: Reliable sparse acknowledgement");
    }

    private static void TestReliableRetransmission()
    {
        DateTime time = DateTime.UtcNow;

        ReliableSender sender = new();

        sender.Send([1], time);

        time += sender.RetransmissionTimeout;

        sender.Tick(time);

        Assert(sender.RetransmissionsAvailable, "Retransmission queued");

        PendingMessage? message = sender.DequeueRetransmission();

        Assert(message != null, "Message exists");
        Assert(message.SequenceId == 0, "Correct sequence");

        Console.WriteLine("PASS: Reliable retransmission");
    }

    private static void TestReliableAcknowledgedNotRetransmitted()
    {
        DateTime time = DateTime.UtcNow;

        ReliableSender sender = new();

        sender.Send([1], time);

        sender.HandleAcknowledgementContiguous(0, time);

        time += sender.RetransmissionTimeout * 2;

        sender.Tick(time);

        Assert(!sender.RetransmissionsAvailable, "No retransmission");

        Console.WriteLine("PASS: ACK prevents retransmission");
    }

    private static void TestReliableRetryLimit()
    {
        DateTime time = DateTime.UtcNow;

        ReliableSender sender = new();

        sender.MaximumRetransmissions = 3;

        sender.Send([1], time);

        for(int i = 0; i < 4; i++)
        {
            time += sender.RetransmissionTimeout;

            sender.Tick(time);

            while(sender.RetransmissionsAvailable)
            {
                sender.DequeueRetransmission();
            }
        }

        Assert(sender.ResetRequired, "Reset required");

        Console.WriteLine("PASS: Retry limit");
    }

    private static void TestReliableRttMeasurement()
    {
        DateTime time = DateTime.UtcNow;

        ReliableSender sender = new();

        sender.Send([1], time);

        time += TimeSpan.FromMilliseconds(100);

        sender.HandleAcknowledgementContiguous(0, time);

        Assert(sender.EstimatedRtt != null, "RTT calculated");

        Assert(sender.EstimatedRtt.Value >= TimeSpan.FromMilliseconds(90), "RTT reasonable");
        Assert(sender.EstimatedRtt.Value <= TimeSpan.FromMilliseconds(110), "RTT reasonable");

        Console.WriteLine("PASS: RTT measurement");
    }

    private static void TestReliableOrderedInOrder()
    {
        ReliableOrderedReceiver receiver = new();

        receiver.Receive(0, [0]);
        receiver.Receive(1, [1]);
        receiver.Receive(2, [2]);

        Assert(receiver.MessagesAvailable, "Messages available");

        Assert(receiver.DequeueMessage()!.Value.SequenceId == 0, "0");
        Assert(receiver.DequeueMessage()!.Value.SequenceId == 1, "1");
        Assert(receiver.DequeueMessage()!.Value.SequenceId == 2, "2");

        Console.WriteLine("PASS: Reliable ordered in order");
    }

    private static void TestReliableOrderedOutOfOrder()
    {
        ReliableOrderedReceiver receiver = new();

        receiver.Receive(0, [0]);
        receiver.Receive(2, [2]);

        Assert(receiver.MessagesAvailable, "0 available");

        Assert(receiver.DequeueMessage()!.Value.SequenceId == 0, "0");

        Assert(!receiver.MessagesAvailable, "2 buffered");

        receiver.Receive(1, [1]);

        Assert(receiver.MessagesAvailable, "1 available");

        Assert(receiver.DequeueMessage()!.Value.SequenceId == 1, "1");
        Assert(receiver.DequeueMessage()!.Value.SequenceId == 2, "2");

        Console.WriteLine("PASS: Reliable ordered out of order");
    }

    private static void TestReliableOrderedGapClosure()
    {
        ReliableOrderedReceiver receiver = new();

        receiver.Receive(2, [2]);
        receiver.Receive(4, [4]);
        receiver.Receive(3, [3]);

        Assert(receiver.BufferedCount == 3, "Three buffered");

        receiver.Receive(0, [0]);
        receiver.Receive(1, [1]);

        Assert(receiver.DequeueMessage()!.Value.SequenceId == 0, "0");
        Assert(receiver.DequeueMessage()!.Value.SequenceId == 1, "1");
        Assert(receiver.DequeueMessage()!.Value.SequenceId == 2, "2");
        Assert(receiver.DequeueMessage()!.Value.SequenceId == 3, "3");
        Assert(receiver.DequeueMessage()!.Value.SequenceId == 4, "4");

        Console.WriteLine("PASS: Reliable ordered gap closure");
    }

    private static void TestUnreliableOrdered()
    {
        UnreliableOrderedReceiver receiver = new();

        Assert(receiver.Receive(5, [5]), "Receive 5");
        Assert(receiver.Receive(7, [7]), "Receive 7");

        Assert(!receiver.Receive(6, [6]), "6 dropped");

        Assert(receiver.Receive(8, [8]), "Receive 8");

        Assert(receiver.DequeueMessage()!.Value.SequenceId == 5, "5");
        Assert(receiver.DequeueMessage()!.Value.SequenceId == 7, "7");
        Assert(receiver.DequeueMessage()!.Value.SequenceId == 8, "8");

        Assert(!receiver.MessagesAvailable, "No more");

        Console.WriteLine("PASS: Unreliable ordered");
    }

    private static void TestReliableOrderedDuplicate()
    {
        ReliableOrderedReceiver receiver = new();

        receiver.Receive(0, [0]);

        Assert(!receiver.Receive(0, [0]), "Duplicate rejected");

        Console.WriteLine("PASS: Reliable ordered duplicate");
    }

    private static void TestSendReliableUnordered()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection client, out Connection server, time);

        Assert(
            client.SendReliableUnordered(
                [1, 2, 3],
                time),
            "Send succeeds");

        client.Tick(time);

        Assert(
            client.PacketsAvailable,
            "Message packet created");

        Packet packet =
            client.DequeuePacket()!;

        Assert(
            packet.Header.PacketType ==
            PacketType.Message,
            "Packet type correct");

        MessagePacket body =
            (MessagePacket)packet.Body!;

        Assert(
            body.Records.Count == 1,
            "One record");

        Assert(
            body.Records[0] is ReliableUnorderedRecord,
            "Record type");

        Console.WriteLine("PASS: Send reliable unordered");
    }

    private static void TestReliableUnorderedDelivery()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection client, out Connection server, time);


        client.SendReliableUnordered(
            [1, 2, 3],
            time);

        client.Tick(time);

        Packet packet =
            client.DequeuePacket()!;

        server.HandlePacket(
            packet,
            time);

        Assert(
            server.ReliableUnorderedMessagesAvailable,
            "Message received");

        byte[]? payload =
            server.DequeueReliableUnorderedMessage();

        Assert(
            payload != null,
            "Payload exists");

        Assert(
            payload.SequenceEqual(new byte[]{1, 2, 3}),
            "Payload correct");

        Console.WriteLine(
            "PASS: Reliable unordered delivery");
    }

    private static void TestReliableAckGeneration()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection client, out Connection server, time);

        MessagePacket packet =
            new MessagePacket();

        packet.Records.Add(
            ReliableUnorderedRecord.Create(
                0,
                [1])!);

        server.HandlePacket(
            new Packet
            {
                Header = new PacketHeader
                {
                    ProtocolVersion =
                        Constants.ProtocolVersion,
                    SessionId = 1,
                    ConnectionToken = 1,
                    PacketType = PacketType.Message
                },
                Body = packet
            },
            time);

        server.Tick(time);

        bool foundAck = false;

        while(server.PacketsAvailable)
        {
            Packet outgoing =
                server.DequeuePacket();

            if(outgoing.Header.PacketType ==
                PacketType.Message)
            {
                MessagePacket body =
                    (MessagePacket)outgoing.Body!;

                foundAck =
                    body.Records.Any(
                        r => r is AckContiguousRecord ||
                            r is AckMaskRecord);
            }
        }

        Assert(foundAck, "ACK generated");

        Console.WriteLine(
            "PASS: Reliable ACK generation");
    }

    private static void TestReliableAcknowledgementFlow()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection client, out Connection server, time);

        client.SendReliableUnordered(
            [1, 2, 3],
            time);

        client.Tick(time);

        Packet data =
            client.DequeuePacket()!;

        server.HandlePacket(
            data,
            time);

        server.Tick(time);

        Packet ack =
            server.DequeuePacket()!;

        client.HandlePacket(
            ack,
            time);

        Assert(
            !client.ReliableUnorderedMessagesAvailable,
            "Message acknowledged");

        Console.WriteLine(
            "PASS: Reliable acknowledgement flow");
    }

    private static void TestReliableOrderedReordering()
    {
        ReliableOrderedReceiver receiver = new();

        receiver.Receive(0, [0]);
        receiver.Receive(2, [2]);

        Assert(receiver.MessagesAvailable, "0 available");

        receiver.DequeueMessage();

        Assert(!receiver.MessagesAvailable, "2 buffered");

        receiver.Receive(1, [1]);

        Assert(receiver.MessagesAvailable, "1 available");

        Assert(receiver.DequeueMessage()!.Value.SequenceId == 1, "1");
        Assert(receiver.DequeueMessage()!.Value.SequenceId == 2, "2");

        Console.WriteLine("PASS: Reliable ordered reordering");
    }

    private static void TestUnreliableOrderedDropsOld()
    {
        UnreliableOrderedReceiver receiver = new();

        Assert(receiver.Receive(0, [0]), "0");
        Assert(receiver.Receive(2, [2]), "2");

        Assert(!receiver.Receive(1, [1]), "1 dropped");

        Console.WriteLine("PASS: Unreliable ordered drop");
    }

    private static void TestReliableOrderedDelivery()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection sender, out Connection receiver, time);

        sender.SendReliableOrdered([1, 2, 3], time);

        sender.Tick(time);

        Packet packet = sender.DequeuePacket()!;

        receiver.HandlePacket(packet, time);

        Assert(
            receiver.ReliableOrderedMessagesAvailable,
            "Message received");

        byte[]? payload =
            receiver.DequeueReliableOrderedMessage();

        Assert(
            payload != null &&
            payload.SequenceEqual(new byte[]{1, 2, 3}),
            "Payload correct");

        Console.WriteLine("PASS: Reliable ordered delivery");
    }

    private static void TestAckRegisterReceiveWindow()
    {
        AckRegister ack = new()
        {
            ReceiveWindowSize = 32
        };

        ack.Receive(0);

        Assert(!ack.Receive(1000), "Outside receive window rejected");

        Assert(ack.Receive(1), "Inside window accepted");

        Console.WriteLine("PASS: Ack receive window");
    }

    private static void TestReliableUnorderedStress()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection sender, out Connection receiver, time);

        const int messageCount = 1000;

        for(int i = 0; i < messageCount; i++)
        {
            sender.SendReliableUnordered(BitConverter.GetBytes(i), time);
        }

        sender.Tick(time);

        List<Packet> packets = [];

        while(sender.PacketsAvailable)
        {
            packets.Add(sender.DequeuePacket()!);
        }

        foreach(Packet packet in packets)
        {
            receiver.HandlePacket(packet, time);
        }

        HashSet<int> received = [];

        while(receiver.ReliableUnorderedMessagesAvailable)
        {
            byte[] payload = receiver.DequeueReliableUnorderedMessage()!;

            received.Add(BitConverter.ToInt32(payload));
        }

        Assert(received.Count == messageCount, "All messages received");

        Console.WriteLine("PASS: Reliable unordered stress");
    }

    private static void TestReliableOrderedStress()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection sender, out Connection receiver, time);


        const int messageCount = 1000;

        for(int i = 0; i < messageCount; i++)
        {
            sender.SendReliableOrdered(BitConverter.GetBytes(i), time);
        }

        sender.Tick(time);

        List<Packet> packets = [];

        while(sender.PacketsAvailable)
        {
            packets.Add(sender.DequeuePacket()!);
        }

        Packet[] packetArray = packets.ToArray();
        
        //new Random().Shuff(packetArray);

        foreach(Packet packet in packetArray)
        {
            receiver.HandlePacket(packet, time);
        }

        int expected = 0;

        while(receiver.ReliableOrderedMessagesAvailable)
        {
            byte[] payload = receiver.DequeueReliableOrderedMessage()!;

            int value = BitConverter.ToInt32(payload);

            Assert(value == expected, $"Expected {expected}, got {value}");

            expected++;
        }

        Assert(expected == messageCount, "All messages delivered");

        Console.WriteLine("PASS: Reliable ordered stress");
    }

    private static void TestSequenceWraparound()
    {
        ReliableSender sender = new();

        typeof(ReliableSender)
            .GetField("NextSequenceId", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .SetValue(sender, uint.MaxValue - 2);

        DateTime time = DateTime.UtcNow;

        uint a = sender.Send([0], time);
        uint b = sender.Send([0], time);
        uint c = sender.Send([0], time);
        uint d = sender.Send([0], time);
        uint e = sender.Send([0], time);

        Assert(a == uint.MaxValue - 2, "A");
        Assert(b == uint.MaxValue - 1, "B");
        Assert(c == uint.MaxValue, "C");
        Assert(d == 0, "D");
        Assert(e == 1, "E");

        Console.WriteLine("PASS: Sequence wraparound");
    }

    private static void TestReliableLossDuplicationStress()
    {
        DateTime time = DateTime.UtcNow;

        CreateConnectedPair(out Connection sender, out Connection receiver, time);

        const int messageCount = 1000;

        for(int i = 0; i < messageCount; i++)
        {
            sender.SendReliableUnordered(BitConverter.GetBytes(i), time);
        }

        Random random = new(1234);

        HashSet<int> delivered = [];

        for(int tick = 0; tick < 1000; tick++)
        {
            sender.Tick(time);
            receiver.Tick(time);

            List<Packet> packets = [];

            while(sender.PacketsAvailable)
            {
                packets.Add(sender.DequeuePacket()!);
            }

            foreach(Packet packet in packets)
            {
                if(random.NextDouble() < 0.1)
                {
                    continue;
                }

                receiver.HandlePacket(packet, time);

                if(random.NextDouble() < 0.1)
                {
                    receiver.HandlePacket(packet, time);
                }
            }

            while(receiver.ReliableUnorderedMessagesAvailable)
            {
                byte[] payload = receiver.DequeueReliableUnorderedMessage()!;

                delivered.Add(BitConverter.ToInt32(payload));
            }

            time += TimeSpan.FromMilliseconds(50);

            if(delivered.Count == messageCount)
            {
                break;
            }
        }

        Assert(delivered.Count == messageCount, "All messages delivered");

        Console.WriteLine("PASS: Reliable loss duplication stress");
    }
}