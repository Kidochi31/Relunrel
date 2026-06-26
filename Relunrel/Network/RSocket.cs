using System.Net;
using System.Net.Sockets;
using Relunrel.Connections;
using Relunrel.Packets;
using SimpleStunner;

namespace Relunrel.Network;

public class RSocket
{
    public Socket Socket;
    public Dictionary<IPEndPoint,Connection> Connections = [];
    
    private Listener Listener = new();
    private bool Listening = false;
    private StunState? StunState;
    public IPEndPoint? ExternalEndPoint {get; private set;}
    public IPEndPoint? InternalEndPoint => Socket.LocalEndPoint as IPEndPoint;
    private Holepunch Holepunch = new();

    private List<IPEndPoint> DeadConnections = [];
    private List<IPEndPoint> NewConnections = [];


    public RSocket()
    {
        Socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
    }

    public void EnableListening()
    {
        Listening = true;
    }
    public void DisableListening()
    {
        Listening = false;
    }

    public Connection ConnectTo(IPEndPoint target, DateTime time)
    {
        if (Connections.ContainsKey(target))
        {
            return Connections[target];
        }
        Connection connection = Connection.CreateActiveConnection((uint)new Random().Next(),target, time);
        Connections[target] = connection;
        return connection;
    }

    public void BeginStun(List<IPEndPoint> resolvedHosts)
    {
        if(StunState is not null)
        {
            return;
        }
        ExternalEndPoint = null;
        StunState = StunAsync.CreateNewStunner(Socket, resolvedHosts, 5, TimeSpan.FromMilliseconds(500));
    }

    public (IPEndPoint[] NewConnections, IPEndPoint[] DeadConnection) Tick(DateTime time)
    {
        TickReceive(time);
        foreach(Connection connection in Connections.Values)
        {
            TickSend(time, connection);
        }
        foreach(IPEndPoint connection in DeadConnections)
        {
            Connections.Remove(connection);
        }
        
        if(StunState is not null)
        {
            StunAsync.Tick(StunState);
        }
        Holepunch.Tick(Socket, time);

        (IPEndPoint[], IPEndPoint[]) returnValue = ([..NewConnections], [..DeadConnections]);
        DeadConnections.Clear();
        NewConnections.Clear();
        return returnValue;
    }

    public void BeginHolePunch(IPEndPoint target, int? timeouts, DateTime time)
    {
        if (Connections.ContainsKey(target))
        {
            return;
        }
        Holepunch.AddTarget(target, timeouts, time);
    }

    public void StopHolePunch(IPEndPoint target)
    {
        Holepunch.RemoveTarget(target);
    }

    private void TickReceive(DateTime time)
    {
        while(Socket.Available > 0)
        {
            byte[] buffer = new byte[2048];

            EndPoint remote = new IPEndPoint(IPAddress.Any, 0);

            int length = Socket.ReceiveFrom(buffer, ref remote);

            Span<byte> data = buffer.AsSpan(0..length);
            IPEndPoint ipRemote = (IPEndPoint)remote;

            if(StunState is not null)
            {
                var result = StunAsync.ReceiveInput(data, StunState, ipRemote);
                if(result is not null)
                {
                    ExternalEndPoint = result;
                    StunState = null;
                }
            }

            Packet? packet = Packet.Deserialize(data);

            
            if(packet is not null)
            {
                if (Connections.ContainsKey(ipRemote))
                {
                    Connections[ipRemote].HandlePacket(packet, time);
                }
                else
                {
                    if(Listener is not null && Listening){
                        // possibly new connection
                        Connection? connection = Listener.HandlePacket(packet, ipRemote, time);
                        if(connection is not null)
                        {
                            Connections[ipRemote] = connection;
                            // remove target from hole punch
                            Holepunch.RemoveTarget(ipRemote);
                            NewConnections.Add(ipRemote);
                        }
                    }
                }
            }
        }
    }

    private void TickSend(DateTime time, Connection connection)
    {
        connection.Tick(time);

        while(connection.PacketsAvailable)
        {
            Packet packet = connection.DequeuePacket()!;

            byte[] bytes = packet.Serialize();

            Socket.SendTo(bytes, connection.RemoteEndPoint);
        }

        if(connection.State == ConnectionState.Disconnected)
        {
            DeadConnections.Add(connection.RemoteEndPoint);
        }
    }
}