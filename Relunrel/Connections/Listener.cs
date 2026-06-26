using System.Net;
using Relunrel.Packets;
namespace Relunrel.Connections;

internal class Listener
{
    public Listener()
    {
        
    }

    public Connection? HandlePacket(Packet packet, IPEndPoint source, DateTime time)
    {
        if(packet.Header.PacketType != PacketType.RequestConnect)
        {
            return null;
        }

        uint SessionId = packet.Header.SessionId;

        uint ConnectionToken = (uint)new Random().Next();

        Connection connection = Connection.CreatePassiveConnection(SessionId, ConnectionToken, source, time);
        return connection;
    }
}