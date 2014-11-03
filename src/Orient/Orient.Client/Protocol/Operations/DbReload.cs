using System;
using System.Collections.Generic;
using System.Text;
using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    class DbReload : IOperation
    {
        public Request Request(int sessionId) {
            var request = new Request();

            // standard request fields
            request.AddDataItem((byte)OperationType.DB_RELOAD);
            request.AddDataItem(sessionId);

            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();

            if (response == null) {
                return document;
            }

            var reader = response.Reader;

            // operation specific fields
            var clusterCount = reader.ReadInt16EndianAware();
            document.SetField("ClusterCount", clusterCount);

            if (clusterCount <= 0) return document;
            var clusters = new List<OCluster>();

            for (var i = 1; i <= clusterCount; i++) {
                var cluster = new OCluster();

                var clusterNameLength = reader.ReadInt32EndianAware();

                cluster.Name = Encoding.Default.GetString(reader.ReadBytes(clusterNameLength));

                cluster.Id = reader.ReadInt16EndianAware();
                if (ServerInfo.ProtocolVersion < 24) {
                    var clusterTypeLength = reader.ReadInt32EndianAware();
                    var clusterType = Encoding.Default.GetString(reader.ReadBytes(clusterTypeLength));
                    cluster.Type = (OClusterType)Enum.Parse(typeof(OClusterType), clusterType, true);
                    cluster.DataSegmentID = (ServerInfo.ProtocolVersion >= 12) ? reader.ReadInt16EndianAware() : (short)0;
                }
                clusters.Add(cluster);
            }

            document.SetField("Clusters", clusters);

            return document;
        }
    }
}
