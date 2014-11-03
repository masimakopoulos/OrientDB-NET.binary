
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal interface IOperation
    {
        Request Request(int sessionId);
        ODocument Response(Response response);
    }
}
