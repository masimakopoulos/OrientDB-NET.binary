using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class DataClusterDrop : IOperation
    {
        public short ClusterId { get; set; }

        public Request Request(int sessionID) {
            var request = new Request {OperationMode = OperationMode.Synchronous};

            // standard request fields
            request.AddDataItem((byte) OperationType.DATACLUSTER_DROP);
            request.AddDataItem(sessionID);

            if (ServerInfo.ProtocolVersion >= 18) {
                request.AddDataItem(ClusterId);
            }
            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();
            if (response == null) {
                return document;
            }

            var reader = response.Reader;
            var removeLocaly = reader.ReadByte() == 1;
            document.SetField("remove_localy", removeLocaly);
            return document;
        }
    }
}