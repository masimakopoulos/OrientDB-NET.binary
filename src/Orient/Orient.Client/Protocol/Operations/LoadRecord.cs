using System;
using System.IO;
using System.Text;
using Orient.Client.API;
using Orient.Client.API.Types;
using Orient.Client.Protocol.Serializers;

namespace Orient.Client.Protocol.Operations
{
    internal class LoadRecord : IOperation
    {
        private readonly ODatabase _database;
        private readonly string _fetchPlan;
        private readonly ORID _orid;

        public LoadRecord(ORID orid, string fetchPlan, ODatabase database) {
            _orid = orid;
            _fetchPlan = fetchPlan;
            _database = database;
        }

        public Request Request(int sessionId) {
            var request = new Request();

            // standard request fields
            request.AddDataItem((byte)OperationType.RECORD_LOAD);
            request.AddDataItem(sessionId);
            request.AddDataItem(_orid);
            request.AddDataItem(_fetchPlan);
            if (ServerInfo.ProtocolVersion >= 9) // Ignore cache 1-true, 0-false
                request.AddDataItem((byte)0);
            if (ServerInfo.ProtocolVersion >= 13) // Load tombstones 1-true , 0-false
                request.AddDataItem((byte)0);

            return request;
        }

        public ODocument Response(Response response) {
            var responseDocument = new ODocument();

            if (response == null) {
                return responseDocument;
            }

            var reader = response.Reader;

            while (true) {
                var payloadStatus = (PayloadStatus)reader.ReadByte();

                var done = false;
                switch (payloadStatus) {
                    case PayloadStatus.NoRemainingRecords:
                        done = true;
                        break;
                    case PayloadStatus.ResultSet:
                        ReadPrimaryResult(responseDocument, reader);
                        break;
                    case PayloadStatus.PreFetched:
                        ReadAssociatedResult(reader);
                        break;
                    default:
                        throw new InvalidOperationException();
                }

                if (done) {
                    break;
                }
            }

            return responseDocument;
        }

        private void ReadAssociatedResult(BinaryReader reader) {
            var zero = reader.ReadInt16EndianAware();
            if (zero != 0)
                throw new InvalidOperationException("Unsupported record format");

            var recordType = (ORecordType)reader.ReadByte();
            if (recordType != ORecordType.Document)
                throw new InvalidOperationException("Unsupported record type");

            var clusterId = reader.ReadInt16EndianAware();
            var clusterPosition = reader.ReadInt64EndianAware();
            var recordVersion = reader.ReadInt32EndianAware();

            var recordLength = reader.ReadInt32EndianAware();
            var record = reader.ReadBytes(recordLength);

            var document = RecordSerializer.Deserialize(new ORID(clusterId, clusterPosition), recordVersion,
                ORecordType.Document, 0, record);

            _database.ClientCache[document.ORID] = document;
        }

        private void ReadPrimaryResult(ODocument responseDocument, BinaryReader reader) {
            responseDocument.SetField("PayloadStatus", PayloadStatus.SingleRecord);

            var contentLength = reader.ReadInt32EndianAware();
            var readBytes = reader.ReadBytes(contentLength);
            var version = reader.ReadInt32EndianAware();
            var recordType = (ORecordType)reader.ReadByte();
            
            var document = new ODocument();

            switch (recordType) {
                case ORecordType.Document:
                    var serialized = Encoding.Default.GetString(readBytes);
                    document = RecordSerializer.Deserialize(serialized);
                    document.ORID = _orid;
                    document.OVersion = version;
                    responseDocument.SetField("Content", document);
                    break;
                case ORecordType.RawBytes:
                    document.SetField("RawBytes", readBytes);
                    responseDocument.SetField("Content", document);
                    break;
                case ORecordType.FlatData:
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }
}