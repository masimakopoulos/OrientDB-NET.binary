using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class DbClose : IOperation
    {
        public Request Request(int sessionId)
        {
            var request = new Request {OperationMode = OperationMode.Asynchronous};

            // standard request fields
            request.AddDataItem((byte)OperationType.DB_CLOSE);
            request.AddDataItem(sessionId);

            return request;
        }

        public ODocument Response(Response response)
        {
            // there are no specific response fields which have to be processed for this operation
            return null;
        }
    }
}
