namespace Relunrel.PacketsTest;
using Relunrel.Packets;
using static Relunrel.Tests.TestHelpers;


public static class Test{
    public static void RunTest()
    {
        Console.WriteLine("Header");
        TestPacketHeader();
        Console.WriteLine("Unrel, Unord");
        TestUnreliableUnordered();
        Console.WriteLine("Rel, Ord");
        TestReliableOrdered();
        Console.WriteLine("AckMask");
        TestAckMask();
        Console.WriteLine("AckCont");
        TestAckContiguous();
        Console.WriteLine("Message");
        TestMessagePacket();
        Console.WriteLine("TruncHeader");
        TestTruncatedHeader();
        Console.WriteLine("InvalidType");
        TestInvalidPacketType();
        Console.WriteLine("Fuzz");
        Fuzz();
        

        Console.WriteLine("random");
        for(int i = 0; i < 1000; i++){
            TestRoundTripRandom();
        }
        Console.WriteLine("done");
    }

    private static void TestPacketHeader()
    {
        PacketHeader header = new()
        {
            ProtocolVersion = 123,
            SessionId = 0x11223344,
            ConnectionToken = 0x55667788,
            PacketType = PacketType.Message
        };

        byte[] buffer = new byte[PacketHeader.HeaderSize];

        PacketWriter writer = new(buffer);

        header.Serialize(ref writer);

        PacketReader reader = new(buffer);

        PacketHeader? result = PacketHeader.Deserialize(ref reader);

        Assert(result != null, "Header deserialized");

        PacketHeader actual = result ?? throw new Exception();

        Assert(actual.ProtocolVersion == header.ProtocolVersion, "ProtocolVersion");
        Assert(actual.SessionId == header.SessionId, "SessionId");
        Assert(actual.ConnectionToken == header.ConnectionToken, "ConnectionToken");
        Assert(actual.PacketType == header.PacketType, "PacketType");
    }

    private static void TestUnreliableUnordered()
    {
        byte[] payload =
        {
            1, 2, 3, 4, 5
        };

        var original =
            UnreliableUnorderedRecord.Create(
                payload);

        Assert(original != null, "Create");

        byte[] buffer =
            new byte[100];

        PacketWriter writer =
            new(buffer);

        original!.Serialize(ref writer);

        PacketReader reader =
            new(buffer.AsSpan(0, writer.BytesWritten));

        Record? record =
            Record.Deserialize(ref reader);

        Assert(record is UnreliableUnorderedRecord, "Type");

        var result =
            (UnreliableUnorderedRecord)record!;

        Assert(result.Payload.SequenceEqual(payload), "Payload");
    }

    private static void TestReliableOrdered()
    {
        byte[] payload =
        {
            10, 20, 30, 40
        };

        var original =
            ReliableOrderedRecord.Create(
                345,
                payload);

        Assert(original != null, "Create");

        byte[] buffer =
            new byte[100];

        PacketWriter writer =
            new(buffer);

        original!.Serialize(ref writer);

        PacketReader reader =
            new(buffer.AsSpan(0, writer.BytesWritten));

        Record? record =
            Record.Deserialize(ref reader);

        Assert(record is ReliableOrderedRecord, "Type");

        var result =
            (ReliableOrderedRecord)record!;

        Assert(result.SequenceId == 345, "SequenceId");
        Assert(result.Payload.SequenceEqual(payload), "Payload");
    }

    private static void TestAckMask()
    {
        AckMaskRecord original =
            new(
                RecordType.AckMaskReliableOrdered,
                100,
                0xAABBCCDDEEFF0011UL);

        byte[] buffer =
            new byte[100];

        PacketWriter writer =
            new(buffer);

        original.Serialize(ref writer);

        PacketReader reader =
            new(buffer.AsSpan(0, writer.BytesWritten));

        Record? record =
            Record.Deserialize(ref reader);

        Assert(record is AckMaskRecord, "Type");

        var result =
            (AckMaskRecord)record!;

        Assert(result.RelativeSequenceId == 100, "RelativeSequenceId");
        Assert(result.AckBitfield == 0xAABBCCDDEEFF0011UL, "AckBitfield");
    }

    private static void TestAckContiguous()
    {
        AckContiguousRecord original =
            new(
                RecordType.AckContiguousReliableOrdered,
                999);

        byte[] buffer =
            new byte[100];

        PacketWriter writer =
            new(buffer);

        original.Serialize(ref writer);

        PacketReader reader =
            new(buffer.AsSpan(0, writer.BytesWritten));

        Record? record =
            Record.Deserialize(ref reader);

        Assert(record is AckContiguousRecord, "Type");

        var result =
            (AckContiguousRecord)record!;

        Assert(result.AcknowledgedSequenceId == 999, "Ack");
    }

    private static void TestMessagePacket()
    {
        MessagePacket body = new();

        body.AddRecord(
            ReliableOrderedRecord.Create(
                10,
                new byte[]
                {
                    1,2,3
                })!);

        body.AddRecord(
            ReliableUnorderedRecord.Create(
                20,
                new byte[]
                {
                    4,5,6
                })!);

        body.AddRecord(
            new AckContiguousRecord(
                RecordType.AckContiguousReliableOrdered,
                9));

        Packet packet = new()
        {
            Header = new PacketHeader
            {
                ProtocolVersion = 1,
                SessionId = 123,
                ConnectionToken = 456,
                PacketType = PacketType.Message
            },
            Body = body
        };

        byte[] data = packet.Serialize();

        Packet? deserialized =
            Packet.Deserialize(data);

        Assert(deserialized != null, "Packet");

        Console.WriteLine(deserialized!.ToDebugString());
    }

    private static void TestTruncatedHeader()
    {
        byte[] data =
        {
            1, 2, 3
        };

        Assert(
            Packet.Deserialize(data) == null,
            "Truncated header");
    }

    private static void TestInvalidPacketType()
    {
        byte[] data =
        {
            1,
            0,0,0,0,
            0,0,0,0,
            255
        };

        Assert(
            Packet.Deserialize(data) == null,
            "Invalid packet type");
    }

    private static void Fuzz()
    {
        Random random = new(1234);

        for(int i = 0; i < 100000; i++)
        {
            byte[] data =
                new byte[random.Next(0, 200)];

            random.NextBytes(data);

            try
            {
                Packet.Deserialize(data);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
                Environment.Exit(1);
            }
        }
    }








    private static void AssertEqual(Packet expected, Packet actual)
    {
        Assert(expected.Header.ProtocolVersion == actual.Header.ProtocolVersion, "ProtocolVersion");
        Assert(expected.Header.SessionId == actual.Header.SessionId, "SessionId");
        Assert(expected.Header.ConnectionToken == actual.Header.ConnectionToken, "ConnectionToken");
        Assert(expected.Header.PacketType == actual.Header.PacketType, "PacketType");

        if(expected.Body == null || actual.Body == null)
        {
            Assert(expected.Body == actual.Body, "Body");
            return;
        }

        Assert(expected.Body.GetType() == actual.Body.GetType(), "BodyType");

        MessagePacket expectedMessage = (MessagePacket)expected.Body;
        MessagePacket actualMessage = (MessagePacket)actual.Body;

        Assert(expectedMessage.Records.Count == actualMessage.Records.Count, "RecordCount");

        for(int i = 0; i < expectedMessage.Records.Count; i++)
        {
            AssertEqual(expectedMessage.Records[i], actualMessage.Records[i]);
        }
    }

    private static void AssertEqual(Record expected, Record actual)
    {
        Assert(expected.Type == actual.Type, "RecordType");

        switch(expected)
        {
            case UnreliableUnorderedRecord a:
            {
                Assert(actual is UnreliableUnorderedRecord, "RecordClass");

                UnreliableUnorderedRecord b =
                    (UnreliableUnorderedRecord)actual;

                Assert(a.Payload.SequenceEqual(b.Payload), "Payload");

                break;
            }

            case UnreliableOrderedRecord a:
            {
                Assert(actual is UnreliableOrderedRecord, "RecordClass");

                UnreliableOrderedRecord b =
                    (UnreliableOrderedRecord)actual;

                Assert(a.SequenceId == b.SequenceId, "SequenceId");
                Assert(a.Payload.SequenceEqual(b.Payload), "Payload");

                break;
            }

            case ReliableUnorderedRecord a:
            {
                Assert(actual is ReliableUnorderedRecord, "RecordClass");

                ReliableUnorderedRecord b =
                    (ReliableUnorderedRecord)actual;

                Assert(a.SequenceId == b.SequenceId, "SequenceId");
                Assert(a.Payload.SequenceEqual(b.Payload), "Payload");

                break;
            }

            case ReliableOrderedRecord a:
            {
                Assert(actual is ReliableOrderedRecord, "RecordClass");

                ReliableOrderedRecord b =
                    (ReliableOrderedRecord)actual;

                Assert(a.SequenceId == b.SequenceId, "SequenceId");
                Assert(a.Payload.SequenceEqual(b.Payload), "Payload");

                break;
            }

            case AckMaskRecord a:
            {
                Assert(actual is AckMaskRecord, "RecordClass");

                AckMaskRecord b =
                    (AckMaskRecord)actual;

                Assert(a.RelativeSequenceId == b.RelativeSequenceId, "RelativeSequenceId");
                Assert(a.AckBitfield == b.AckBitfield, "AckBitfield");

                break;
            }

            case AckContiguousRecord a:
            {
                Assert(actual is AckContiguousRecord, "RecordClass");

                AckContiguousRecord b =
                    (AckContiguousRecord)actual;

                Assert(a.AcknowledgedSequenceId == b.AcknowledgedSequenceId, "AcknowledgedSequenceId");

                break;
            }

            default:
            {
                Assert(false, $"Unhandled record type: {expected.GetType().Name}");
                break;
            }
        }
    }

    private static byte[] RandomPayload(Random random)
    {
        int length = random.Next(0, 256);

        byte[] payload = new byte[length];

        random.NextBytes(payload);

        return payload;
    }

    private static Record RandomRecord(Random random)
    {
        uint sequence = (uint)random.NextInt64(uint.MaxValue + 1L);

        switch(random.Next(6))
        {
            case 0:
                return UnreliableUnorderedRecord.Create(
                    RandomPayload(random))!;

            case 1:
                return UnreliableOrderedRecord.Create(
                    sequence,
                    RandomPayload(random))!;

            case 2:
                return ReliableUnorderedRecord.Create(
                    sequence,
                    RandomPayload(random))!;

            case 3:
                return ReliableOrderedRecord.Create(
                    sequence,
                    RandomPayload(random))!;

            case 4:
                return new AckMaskRecord(
                    (RecordType)random.Next(
                        (int)RecordType.AckMaskReliableOrdered,
                        (int)RecordType.AckMaskReliableUnordered + 1),
                    sequence,
                    (ulong)random.NextInt64());

            default:
                return new AckContiguousRecord(
                    (RecordType)random.Next(
                        (int)RecordType.AckContiguousReliableOrdered,
                        (int)RecordType.AckContiguousReliableUnordered + 1),
                    sequence);
        }
    }

    private static Packet RandomPacket(Random random)
    {
        PacketType type =
            (PacketType)random.Next(
                (int)PacketType.RequestConnect,
                (int)PacketType.Heartbeat + 1);

        Packet packet = new()
        {
            Header = new PacketHeader
            {
                ProtocolVersion = (byte)random.Next(256),
                SessionId = (uint)random.NextInt64(uint.MaxValue + 1L),
                ConnectionToken = (uint)random.NextInt64(uint.MaxValue + 1L),
                PacketType = type
            }
        };

        if(type == PacketType.Message)
        {
            MessagePacket body = new();

            int count = random.Next(0, 20);

            for(int i = 0; i < count; i++)
            {
                Record record = RandomRecord(random);

                if(!body.AddRecord(record))
                {
                    break;
                }
            }

            packet.Body = body;
        }

        return packet;
    }

    private static void TestRoundTripRandom()
    {
        Random random = new(12345);

        for(int i = 0; i < 100000; i++)
        {
            Packet original = RandomPacket(random);

            byte[] data = original.Serialize();

            Packet? result = Packet.Deserialize(data);

            Assert(result != null, $"Deserialize {i}");

            AssertEqual(original, result!);

            byte[] first = original.Serialize();

            Packet packet = Packet.Deserialize(first)!;

            byte[] second = packet.Serialize();

            Assert(first.SequenceEqual(second), "Byte-for-byte roundtrip");
        }

        Console.WriteLine("PASS: Random round-trip");
    }
}


