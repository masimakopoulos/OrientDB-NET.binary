using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class DbExist : IOperation
    {
        internal string DatabaseName { get; set; }
        internal OStorageType StorageType { get; set; }

        public Request Request(int sessionId) {
            var request = new Request();

            // standard request fields
            request.AddDataItem((byte)OperationType.DB_EXIST);
            request.AddDataItem(sessionId);
            // operation specific fields
            request.AddDataItem(DatabaseName);
            if (ServerInfo.ProtocolVersion >= 16) //since 1.5 snapshot but not in 1.5
                request.AddDataItem(StorageType.ToString().ToLower());

            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();

            if (response == null) {
                return document;
            }

            var reader = response.Reader;

            // operation specific fields
            var existByte = reader.ReadByte();

            document.SetField("Exists", existByte != 0);

            return document;
        }
    }
}
