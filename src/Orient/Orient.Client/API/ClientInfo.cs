using Orient.Client.API.Types;

namespace Orient.Client.API
{
    public static class ClientInfo
    {
        internal static string ClientId {
            get { return "null"; }
        }

        internal static string DriverName {
            get { return "OrientDB-NET.binary"; }
        }

        internal static string DriverVersion {
            get { return "0.2.1"; }
        }

        internal static short ProtocolVersion {
            get { return 25; }
        }
        internal static RecordFormat SerializationImpl {
            get { return RecordFormat.ORecordDocument2csv; }
        }
    }
}
