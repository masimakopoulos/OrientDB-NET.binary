using Orient.Client.API.Types;
using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;

namespace Orient.Client.API.Query
{
    public class ORecordMetadata
    {
        private readonly Connection _connection;

        public ORID ORID { get; private set; }

        public int OVersion { get; private set; }

        internal ORecordMetadata(Connection connection) {
            _connection = connection;
        }

        public ORecordMetadata SetORID(ORID orid) {
            ORID = orid;
            return this;
        }

        public ORecordMetadata Run() {
            var operation = new RecordMetadata(ORID);
            var document = _connection.ExecuteOperation(operation);
            ORID = document.ORID;
            OVersion = document.OVersion;
            return this;
        }
    }
}