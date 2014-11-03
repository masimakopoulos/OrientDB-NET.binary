using System.IO;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    /// <summary>
    /// Gets metadata from a record. 
    /// </summary>
    internal class RecordMetadata : IOperation
    {
        public ORID ORID { get; set; }

        public RecordMetadata(ORID rid) {
            ORID = rid;
        }

        public Request Request(int sessionID) {
            var request = new Request();

            request.AddDataItem((byte) OperationType.RECORD_METADATA);
            request.AddDataItem(sessionID);
            request.AddDataItem(ORID);

            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();

            if (response == null) {
                return document;
            }

            var reader = response.Reader;
            document.ORID = ReadORID(reader);
            document.OVersion = reader.ReadInt32EndianAware();

            return document;
        }

        private ORID ReadORID(BinaryReader reader) {
            var result = new ORID {
                ClusterId = reader.ReadInt16EndianAware(),
                ClusterPosition = reader.ReadInt64EndianAware()
            };
            return result;
        }
    }
}
