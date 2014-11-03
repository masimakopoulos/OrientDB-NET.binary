using System;
using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class UpdateRecord : IOperation
    {
        private readonly ODocument _document;

        private const char RecordType = 'd';// record type b=raw, f=flat, d=document
        private const int Mode = 0; // transmission mode 0=synchronous, 1=asynchronous

        public UpdateRecord(ODocument document) {
            _document = document;
        }

        public Request Request(int sessionId) {
            var request = new Request();

            if (_document.ORID == null)
                throw new InvalidOperationException();

            CorrectClassName();


            // standard request fields
            request.AddDataItem((byte)OperationType.RECORD_UPDATE);
            request.AddDataItem(sessionId);

            request.AddDataItem(_document.ORID.ClusterId); // cluster id
            request.AddDataItem(_document.ORID.ClusterPosition);
            if (ServerInfo.ProtocolVersion >= 23)
                request.AddDataItem((byte)1); // update content 1=true 0=false
            request.AddDataItem(_document.Serialize()); // content
            request.AddDataItem(_document.OVersion); // record version
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

            _document.OVersion = reader.ReadInt32EndianAware(); // record version

            if (ServerInfo.ProtocolVersion <= 23) return responseDocument;
            var countOfCollectionChanges = reader.ReadInt32EndianAware();
            if (countOfCollectionChanges <= 0) return responseDocument;
            for (var i = 0; i < countOfCollectionChanges; i++) {
                var mostSigBits = reader.ReadInt64EndianAware();
                var leastSigBits = reader.ReadInt64EndianAware();
                var updatedFileId = reader.ReadInt64EndianAware();
                var updatedPageIndex = reader.ReadInt64EndianAware();
                var updatedPageOffset = reader.ReadInt32EndianAware();
            }

            return responseDocument;


        }
    }
}
