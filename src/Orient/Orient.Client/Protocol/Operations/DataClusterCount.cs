﻿using System;
using System.Collections.Generic;
using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class DataClusterCount : IOperation
    {
        public List<short> Clusters { get; set; }
        public Boolean CountTombStones;

        public Request Request(int sessionID) {
            var request = new Request {OperationMode = OperationMode.Synchronous};

            // standard request fields
            request.AddDataItem((byte) OperationType.DATACLUSTER_COUNT);
            request.AddDataItem(sessionID);

            request.AddDataItem((short) Clusters.Count);
            foreach (var item in Clusters) {
                request.AddDataItem(item);
            }

            /*
             * count-tombstones the flag which indicates whether deleted records should be taken in account. 
             * It is applicable for autosharded storage only, otherwise it is ignored.
             */
            if (ServerInfo.ProtocolVersion >= 13)
                request.AddDataItem((CountTombStones) ? (byte) 1 : (byte) 0);

            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();
            if (response == null) {
                return document;
            }

            var reader = response.Reader;
            var size = reader.ReadInt64EndianAware();
            document.SetField("Count", size);

            return document;
        }
    }
}