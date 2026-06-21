

using System.Net;
using System.Net.Sockets;

internal class Holepunch
{
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);
    static readonly byte[] HolePunchData = [0x52, 0x48];
    public Dictionary<IPEndPoint, (DateTime nextSend, int? remainingAttempts)> Targets = new();

    public void AddTarget(IPEndPoint target, int? timeouts, DateTime time)
    {
        Targets[target] = (time, timeouts);
    }

    public void RemoveTarget(IPEndPoint target)
    {
        if (Targets.ContainsKey(target))
        {
            Targets.Remove(target);
        }
    }

    public void Tick(Socket socket, DateTime time)
    {
        List<IPEndPoint> targets = Targets.Keys.ToList();
        foreach(var target in targets)
        {
            (DateTime nextSend, int? remainingAttempts) = Targets[target];
            if(nextSend <= time)
            {
                socket.SendTo(HolePunchData, target);
                if(remainingAttempts is not null){
                    int remainingAttemptsInt = remainingAttempts.Value - 1;
                    if(remainingAttemptsInt <= 0)
                    {
                        Targets.Remove(target);
                    }
                    else
                    {
                        Targets[target] = (time + Timeout, remainingAttemptsInt);
                    }
                }
            }
        }
    }
}