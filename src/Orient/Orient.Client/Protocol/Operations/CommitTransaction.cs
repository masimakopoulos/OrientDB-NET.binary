using System;
using System.Collections.Generic;
using System.IO;
using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class CommitTransaction : IOperation
    {
        private readonly ODatabase _database;
        private readonly List<TransactionRecord> _records;

        public CommitTransaction(List<TransactionRecord> records, ODatabase database) {
            _records = records;
            _database = database;
        }

        public Request Request(int sessionId) {
            var request = new Request();
            const int transactionId = 1;
            // standard request fields
            request.AddDataItem((byte)OperationType.TX_COMMIT);
            request.AddDataItem(sessionId);


            request.AddDataItem(transactionId); // tx-id the transaction Id
            request.AddDataItem((byte)0); // tells if the server must use the Transaction Log to recover the transaction. 1 = true, 0 = false

            foreach (var item in _records) // list of transaction records
                item.AddToRequest(request);

            request.AddDataItem((byte)0); // zero terminated end of records

            request.AddDataItem(0);
            return request;
        }


        public ODocument Response(Response response) {
            var responseDocument = new ODocument();

            var reader = response.Reader;

            var createdRecordMapping = new Dictionary<ORID, ORID>();
            var recordCount = reader.ReadInt32EndianAware();
            for (var i = 0; i < recordCount; i++) {
                var tempORID = ReadORID(reader);
                var realORID = ReadORID(reader);
                createdRecordMapping.Add(tempORID, realORID);
            }
            responseDocument.SetField("CreatedRecordMapping", createdRecordMapping);

            var updatedRecordCount = reader.ReadInt32EndianAware();
            var updateRecordVersions = new Dictionary<ORID, int>();
            for (var i = 0; i < updatedRecordCount; i++) {
                var updatedORID = ReadORID(reader);
                var newRecordVersion = reader.ReadInt32EndianAware();
                updateRecordVersions.Add(updatedORID, newRecordVersion);
            }
            responseDocument.SetField("UpdatedRecordVersions", updateRecordVersions);

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
                // reader is prematurely empty for some reason.
            }
            

            return responseDocument;
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