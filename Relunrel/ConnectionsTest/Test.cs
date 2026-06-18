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
        TestReliableUnorderedChannel();
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

    private static void TestReliableUnorderedChannel()
    {
        ReliableUnorderedChannel sender = new();
        ReliableUnorderedChannel receiver = new();

        ReliableUnorderedRecord message =
            sender.Send(new byte[]
            {
                1, 2, 3, 4
            });

        receiver.HandleReliableMessage(message);

        Assert(receiver.ReceivedMessageCount == 1, "Message received");

        AckContiguousRecord ack =
            receiver.BuildAck();

        sender.HandleAck(ack);

        Assert(sender.PendingMessageCount == 0, "Message acknowledged");

        Assert(receiver.TryDequeueMessage(out byte[] payload), "Payload dequeued");

        Assert(payload.SequenceEqual(new byte[]
        {
            1, 2, 3, 4
        }), "Payload correct");

        Console.WriteLine("PASS: ReliableUnorderedChannel");
    }
}