using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class Connect : IOperation
    {
        internal string UserName { private get; set; }
        internal string UserPassword { private get; set; }

        public Request Request(int sessionId) {
            var request = new Request();

            // standard request fields
            request.AddDataItem((byte)OperationType.CONNECT);
            request.AddDataItem(sessionId);
            // operation specific fields
            if (ServerInfo.ProtocolVersion > 7) {
                request.AddDataItem(ClientInfo.DriverName);
                request.AddDataItem(ClientInfo.DriverVersion);
                request.AddDataItem(ClientInfo.ProtocolVersion);
                request.AddDataItem(ClientInfo.ClientId);    
            }
            if (ServerInfo.ProtocolVersion > 22)
                request.AddDataItem(ClientInfo.SerializationImpl.ToString());
            request.AddDataItem(UserName);
            request.AddDataItem(UserPassword);

            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();

            if (response == null) {
                return document;
            }

            var reader = response.Reader;

            // operation specific fields
            document.SetField("SessionId", reader.ReadInt32EndianAware());

            return document;
        }
    }
}
