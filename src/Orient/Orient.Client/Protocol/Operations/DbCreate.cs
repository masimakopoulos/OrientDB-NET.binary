using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class DbCreate : IOperation
    {
        internal string DatabaseName { private get; set; }
        internal ODatabaseType DatabaseType { private get; set; }
        internal OStorageType StorageType { private get; set; }

        public Request Request(int sessionId)
        {
            var request = new Request();
            // standard request fields
            request.AddDataItem((byte)OperationType.DB_CREATE);
            request.AddDataItem(sessionId);
            // operation specific fields
            request.AddDataItem(DatabaseName);
            request.AddDataItem(DatabaseType.ToString().ToLower());
            request.AddDataItem(StorageType.ToString().ToLower());

            return request;
        }

        public ODocument Response(Response response)
        {
            var document = new ODocument();

            if (response == null)
            {
                return document;
            }

            document.SetField("IsCreated", response.Status == ResponseStatus.OK);

            return document;
        }
    }
}
