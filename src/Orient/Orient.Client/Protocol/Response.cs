using System.IO;
using System.Text;
using Orient.Client.API;
using Orient.Client.API.Exceptions;

namespace Orient.Client.Protocol
{
    internal class Response
    {
        internal ResponseStatus Status { get; private set; }
        private int SessionId { get; set; }
        internal Connection Connection { get; private set; }
        internal BinaryReader Reader { get; private set; }

        public Response(Connection connection) {
            Connection = connection;
        }

        public void Receive() {
            Reader = new BinaryReader(Connection.GetNetworkStream());
            var reader = Reader;
            Status = (ResponseStatus)reader.ReadByte();
            SessionId = reader.ReadInt32EndianAware();

            if (Status != ResponseStatus.ERROR) return;

            var exceptionString = "";

            var followByte = reader.ReadByte();

            while (followByte == 1) {
                var exceptionClassLength = reader.ReadInt32EndianAware();
                exceptionString += Encoding.Default.GetString(reader.ReadBytes(exceptionClassLength)) + ": ";

                var exceptionMessageLength = reader.ReadInt32EndianAware();

                // don't read exception message string if it's null
                if (exceptionMessageLength != -1) {
                    exceptionString += Encoding.Default.GetString(reader.ReadBytes(exceptionMessageLength)) + "\n";
                }

                followByte = reader.ReadByte();
            }
            if (ServerInfo.ProtocolVersion >= 19) {
                 var serializedVersionLength = reader.ReadInt32EndianAware();
                 var buffer = reader.ReadBytes(serializedVersionLength);
            }

            throw new OException(OExceptionType.Operation, exceptionString);
        }
    }
}
