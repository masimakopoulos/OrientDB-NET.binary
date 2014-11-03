using System;
using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class CreateRecord : IOperation
    {
        private readonly ODocument _document;
        private readonly ODatabase _database;

        private const int DataSegmentId = -1;
        private const int ClusterId = -1;
        private const char RecordType = 'd';// record type b=raw, f=flat, d=document
        private const int Mode = 0; // transmission mode 0=synchronous, 1=asynchronous

        public CreateRecord(ODocument document, ODatabase database) {
            _document = document;
            _database = database;
        }

        public Request Request(int sessionId) {
            var request = new Request();

            if (_document.ORID != null)
                throw new InvalidOperationException();

            CorrectClassName();

            var clusterId = _database.GetClusterIdFor(_document.OClassName);
            _document.ORID = new ORID(clusterId, -1);

            // standard request fields
            request.AddDataItem((byte)OperationType.RECORD_CREATE);
            request.AddDataItem(sessionId);
            if (ServerInfo.ProtocolVersion < 24)
                request.AddDataItem(DataSegmentId);  // data segment id (Removed in protocol v24)
            request.AddDataItem((short)ClusterId); // cluster id
            request.AddDataItem(_document.Serialize());
            request.AddDataItem((byte)RecordType);
            request.AddDataItem((byte)Mode);

            return request;
        }

        private void CorrectClassName() {
            if (_document.OClassName == "OVertex")
                _document.OClassName = "V";
            if (_document.OClassName == "OEdge")
                _document.OClassName = "E";
        }

        public ODocument Response(Response response) {
            var responseDocument = _document;


            if (response == null) {
                return responseDocument;
            }

            var reader = response.Reader;

            _document.ORID.ClusterPosition = reader.ReadInt64EndianAware(); // cluster position
            if(ServerInfo.ProtocolVersion >= 11)
                _document.OVersion = reader.ReadInt32EndianAware(); // record version

            if (ServerInfo.ProtocolVersion <= 23) return responseDocument;
            try { //if (reader.BaseStream.CanRead && reader.PeekChar() != -1)
                var collectionChangesCount = reader.ReadInt32EndianAware();
                if (collectionChangesCount <= 0) return responseDocument;
                throw new NotSupportedException("Processing of collection changes is not implemented. Failing rather than ignoring potentially significant data");
                //for (var i = 0; i < collectionChangesCount; i++) {
                //    var mostSigBitsOfId = reader.ReadInt64EndianAware();
                //    var leastSigBitsOfId = reader.ReadInt64EndianAware();
                //    var updatedFileId = reader.ReadInt64EndianAware();
                //    var updatedPageIndex = reader.ReadInt64EndianAware();
                //    var updatedPageOffset = reader.ReadInt32EndianAware();
                //}
            }
            catch (Exception) {
                
            }
            

            return responseDocument;


        }
    }
}
