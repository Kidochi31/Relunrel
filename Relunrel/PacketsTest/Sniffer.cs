namespace Relunrel.PacketsTest;

using System.Net;
using System.Net.Sockets;
using Relunrel.Packets;
using static Relunrel.Tests.TestHelpers;


public static class Sniffer{
    public static void RunSniffer()
    {
        Console.WriteLine("Initialising sniffer...");
        Socket socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);
        socket.SendTo([0,0], new IPEndPoint(IPAddress.Loopback, 49000));
        Console.WriteLine(socket.LocalEndPoint);
        IPEndPoint target1 = GetTarget()!;
        IPEndPoint target2 = GetTarget()!;

        socket.ReceiveTimeout = 0;
        while (true)
        {
            byte[] buffer = new byte[2048];

            EndPoint remote = new IPEndPoint(IPAddress.Any, 0);

            int length = socket.ReceiveFrom(buffer, ref remote);

            Span<byte> data = buffer.AsSpan(0..length);
            IPEndPoint ipRemote = (IPEndPoint)remote;

            Packet? packet = Packet.Deserialize(data);
            
            

            if(target1.Equals(ipRemote))
            {
                Console.WriteLine($"{target1.Address} {target1.Port} -> {target2.Address} {target2.Port}");
                socket.SendTo(data, target2);
            }
            else if (target2.Equals(ipRemote))
            {
                Console.WriteLine($"{target2.Address} {target2.Port} -> {target1.Address} {target1.Port}");
                socket.SendTo(data, target1);
            }
            else
            {
                Console.WriteLine($"unknown source: {ipRemote.Address} {ipRemote.Port}");
            }

            if(packet is not null)
            {
                Console.WriteLine(packet.ToDebugString());
            }
            else
            {
                Console.WriteLine(string.Join(", ", buffer[..length]));
            }
        }
    }

    static IPEndPoint? GetTarget()
    {
        Console.Write("Enter IP Port: ");
        string? input = Console.ReadLine();
        while(input is null)
        {
            Console.Write("Enter IP Port: ");
            input = Console.ReadLine();
        }
        //string[] cmd = input.Split();
        //string enteredIp = cmd[0];
        string enteredPort = input;
        // validIp = IPAddress.TryParse(enteredIp, out IPAddress? ip);
        bool validPort = int.TryParse(enteredPort, out int port);
        
        return new IPEndPoint(IPAddress.Parse("127.0.0.1"), port);
    }
}


