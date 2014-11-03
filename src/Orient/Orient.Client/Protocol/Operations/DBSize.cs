using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class DbSize : IOperation
    {
        internal string DatabaseName { get; set; }
        internal OStorageType StorageType { get; set; }

        public Request Request(int sessionId) {
            var request = new Request {OperationMode = OperationMode.Synchronous};

            // standard request fields
            request.AddDataItem((byte)OperationType.DB_SIZE);
            request.AddDataItem(sessionId);
            // operation specific fields

            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();

            if (response == null) {
                return document;
            }

            var reader = response.Reader;

            // operation specific fields
            var size = reader.ReadInt64EndianAware();

            document.SetField("Size", size);
            return document;
        }
    }
}
