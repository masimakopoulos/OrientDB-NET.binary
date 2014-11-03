using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class DataClusterDataRange : IOperation
    {
        public short ClusterId { get; set; }

        public Request Request(int sessionID) {
            var request = new Request {OperationMode = OperationMode.Synchronous};

            // standard request fields
            request.AddDataItem((byte) OperationType.DATACLUSTER_DATARANGE);
            request.AddDataItem(sessionID);
            request.AddDataItem(ClusterId);

            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();
            if (response == null) {
                return document;
            }

            var reader = response.Reader;
            var begin = reader.ReadInt64EndianAware();
            var end = reader.ReadInt64EndianAware();
            var embededDoc = new ODocument();
            embededDoc.SetField("Begin", begin);
            embededDoc.SetField("End", end);
            document.SetField("Content", embededDoc);

            return document;
        }
    }
}