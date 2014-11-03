using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class DataClusterAdd : IOperation
    {
        public OClusterType ClusterType { get; set; }

        public string ClusterName { get; set; }

        public Request Request(int sessionID) {
            var request = new Request {OperationMode = OperationMode.Synchronous};

            // standard request fields
            request.AddDataItem((byte) OperationType.DATACLUSTER_ADD);
            request.AddDataItem(sessionID);

            if (ServerInfo.ProtocolVersion < 24)
                request.AddDataItem(ClusterType.ToString().ToUpper());

            request.AddDataItem(ClusterName);

            if (ServerInfo.ProtocolVersion >= 18) {
                request.AddDataItem((short) -1); //clusterid
            }
            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();
            if (response == null) {
                return document;
            }

            var reader = response.Reader;
            var clusterid = reader.ReadInt16EndianAware();
            document.SetField("ClusterId", clusterid);

            return document;
        }
    }
}