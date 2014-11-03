using Orient.Client.API.Types;
using Orient.Client.Protocol.Serializers;

namespace Orient.Client.Protocol.Operations
{
    internal class DBList : IOperation
    {
        public Request Request(int sessionID) {
            var request = new Request();
            request.AddDataItem((byte) OperationType.DB_LIST);
            request.AddDataItem(sessionID);
            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();
            if (response == null) {
                return document;
            }
            var reader = response.Reader;
            var recordLength = reader.ReadInt32EndianAware();
            var rawRecord = reader.ReadBytes(recordLength);
            document = RecordSerializer.Deserialize(BinarySerializer.ToString(rawRecord).Trim(), document);
            return document;
        }
    }
}