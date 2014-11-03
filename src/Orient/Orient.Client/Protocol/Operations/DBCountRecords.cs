using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class DbCountRecords : IOperation
    {
        public Request Request(int sessionID)
        {
            var request = new Request {OperationMode = OperationMode.Synchronous};

            // standard request fields
            request.AddDataItem((byte)OperationType.DB_COUNTRECORDS);
            request.AddDataItem(sessionID);

            return request;
        }

        public ODocument Response(Response response)
        {
            var document = new ODocument();
            if (response == null)
            {
                return document;
            }

            var reader = response.Reader;
            var size = reader.ReadInt64EndianAware();
            document.SetField("Count", size);

            return document;
        }
    }
}
