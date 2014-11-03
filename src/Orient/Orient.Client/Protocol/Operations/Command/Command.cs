﻿using System.Collections.Generic;
using System.IO;
using Orient.Client.API;
using Orient.Client.API.Exceptions;
using Orient.Client.API.Types;
using Orient.Client.Protocol.Serializers;

namespace Orient.Client.Protocol.Operations.Command
{
    internal class Command : IOperation
    {
        internal OperationMode OperationMode { get; set; }
        internal CommandPayloadBase CommandPayload { get; set; }

        public Request Request(int sessionId) {
            var request = new Request();

            // standard request fields
            request.AddDataItem((byte)OperationType.COMMAND);
            request.AddDataItem(sessionId);
            // operation specific fields
            request.AddDataItem((byte)OperationMode);

            // idempotent command (e.g. select)
            var queryPayload = CommandPayload as CommandPayloadQuery;
            if (queryPayload != null) {
                // Write command payload length
                request.AddDataItem(queryPayload.PayLoadLength);
                request.AddDataItem(queryPayload.ClassName);
                //(text:string)(non-text-limit:int)[(fetch-plan:string)](serialized-params:bytes[])
                request.AddDataItem(queryPayload.Text);
                request.AddDataItem(queryPayload.NonTextLimit);
                request.AddDataItem(queryPayload.FetchPlan);

                // TODO: Implement Serialized Params for Idempotent query
                // HACK: 0:int means disable
                request.AddDataItem((int)0);
                return request;
            }
            // non-idempotent command (e.g. insert)
            var scriptPayload = CommandPayload as CommandPayloadScript;
            if (scriptPayload != null) {
                // Write command payload length
                request.AddDataItem(scriptPayload.PayLoadLength);
                request.AddDataItem(scriptPayload.ClassName);
                if (scriptPayload.Language != "gremlin")
                    request.AddDataItem(scriptPayload.Language);
                request.AddDataItem(scriptPayload.Text);
                request.AddDataItem((byte)0);
                request.AddDataItem((byte)0);

                return request;
            }
            var commandPayload = CommandPayload as CommandPayloadCommand;
            if (commandPayload != null) {
                // Write command payload length
                request.AddDataItem(commandPayload.PayLoadLength);
                request.AddDataItem(commandPayload.ClassName);
                // (text:string)(has-simple-parameters:boolean)(simple-paremeters:bytes[])(has-complex-parameters:boolean)(complex-parameters:bytes[])
                request.AddDataItem(commandPayload.Text);
                // has-simple-parameters boolean
                request.AddDataItem((byte)0); // 0 - false, 1 - true
                //request.DataItems.Add(new RequestDataItem() { Type = "int", Data = BinarySerializer.ToArray(0) });
                // has-complex-parameters
                request.AddDataItem((byte)0); // 0 - false, 1 - true
                //request.DataItems.Add(new RequestDataItem() { Type = "int", Data = BinarySerializer.ToArray(0) });
                return request;
            }
            throw new OException(OExceptionType.Operation, "Invalid payload");
        }

        public ODocument Response(Response response) {
            var responseDocument = new ODocument();

            if (response == null) {
                return responseDocument;
            }

            var reader = response.Reader;

            // operation specific fields
            var payloadStatus = (PayloadStatus)reader.ReadByte();

            responseDocument.SetField("PayloadStatus", payloadStatus);

            if (OperationMode == OperationMode.Asynchronous) {
                var documents = new List<ODocument>();

                while (payloadStatus != PayloadStatus.NoRemainingRecords) {
                    var document = ParseDocument(reader);

                    switch (payloadStatus) {
                        case PayloadStatus.ResultSet:
                            documents.Add(document);
                            break;
                        case PayloadStatus.PreFetched:
                            //client cache
                            response.Connection.Database.ClientCache[document.ORID] = document;
                            break;
                        default:
                            break;
                    }

                    payloadStatus = (PayloadStatus)reader.ReadByte();
                }

                responseDocument.SetField("Content", documents);
            } else {
                int contentLength;

                switch (payloadStatus) {
                    case PayloadStatus.NullResult: // 'n'
                        // nothing to do
                        break;
                    case PayloadStatus.SingleRecord: // 'r'
                        var document = ParseDocument(reader);
                        responseDocument.SetField("Content", document);
                        break;
                    case PayloadStatus.SerializedResult: // 'a'
                        // TODO: how to parse result - string?
                        contentLength = reader.ReadInt32EndianAware();
                        var serialized = System.Text.Encoding.Default.GetString(reader.ReadBytes(contentLength));
                        responseDocument.SetField("Content", serialized);
                        break;
                    case PayloadStatus.RecordCollection: // 'l'
                        var documents = new List<ODocument>();

                        var recordsCount = reader.ReadInt32EndianAware();

                        for (var i = 0; i < recordsCount; i++) {
                            documents.Add(ParseDocument(reader));
                        }

                        responseDocument.SetField("Content", documents);
                        break;
                    default:
                        break;
                }

                if (ServerInfo.ProtocolVersion >= 17) {
                    //Load the fetched records in cache
                    while ((payloadStatus = (PayloadStatus)reader.ReadByte()) != PayloadStatus.NoRemainingRecords) {
                        var document = ParseDocument(reader);
                        if (document != null && payloadStatus == PayloadStatus.PreFetched) {
                            //Put in the client local cache
                            response.Connection.Database.ClientCache[document.ORID] = document;
                        }
                    }
                }
            }

            return responseDocument;
        }

        private ODocument ParseDocument(BinaryReader reader) {
            ODocument document = null;

            var classId = reader.ReadInt16EndianAware();

            if (classId == -2) // NULL
            {
            } else if (classId == -3) // record id
            {
                var orid = new ORID();
                orid.ClusterId = reader.ReadInt16EndianAware();
                orid.ClusterPosition = reader.ReadInt64EndianAware();

                document = new ODocument();
                document.ORID = orid;
                document.OClassId = classId;
            } else {
                var type = (ORecordType)reader.ReadByte();

                var orid = new ORID();
                orid.ClusterId = reader.ReadInt16EndianAware();
                orid.ClusterPosition = reader.ReadInt64EndianAware();
                var version = reader.ReadInt32EndianAware();
                var recordLength = reader.ReadInt32EndianAware();
                var rawRecord = reader.ReadBytes(recordLength);
                document = RecordSerializer.Deserialize(orid, version, type, classId, rawRecord);
            }

            return document;
        }
    }
}
