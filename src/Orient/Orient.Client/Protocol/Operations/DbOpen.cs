using System;
using System.Collections.Generic;
using System.Text;
using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class DbOpen : IOperation
    {
        internal string DatabaseName { get; set; }
        internal ODatabaseType DatabaseType { get; set; }
        internal string UserName { get; set; }
        internal string UserPassword { get; set; }
        internal string ClusterConfig { get { return "null"; } }

        public Request Request(int sessionId) {
            var request = new Request();
            // standard request fields
            request.AddDataItem((byte)OperationType.DB_OPEN);
            request.AddDataItem(sessionId);
            // operation specific fields
            if (ServerInfo.ProtocolVersion > 7) {
                request.AddDataItem(ClientInfo.DriverName);
                request.AddDataItem(ClientInfo.DriverVersion);
                request.AddDataItem(ClientInfo.ProtocolVersion);
                request.AddDataItem(ClientInfo.ClientId);
            }
            if (ServerInfo.ProtocolVersion > 21)
                request.AddDataItem(ClientInfo.SerializationImpl.ToString());
            request.AddDataItem(DatabaseName);
            if (ServerInfo.ProtocolVersion >= 8)
                request.AddDataItem(DatabaseType.ToString().ToLower());
            request.AddDataItem(UserName);
            request.AddDataItem(UserPassword);

            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();

            if (response == null) {
                return document;
            }

            var reader = response.Reader;

            // operation specific fields
            document.SetField("SessionId", reader.ReadInt32EndianAware());
            
            var clusterCount = ServerInfo.ProtocolVersion >= 7 ? reader.ReadInt16EndianAware() : reader.ReadInt32EndianAware();

            document.SetField("ClusterCount", clusterCount);

            if (clusterCount > 0) {
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
                        cluster.DataSegmentID = ServerInfo.ProtocolVersion >= 12 ? reader.ReadInt16EndianAware() : (short)0;
                    }
                    clusters.Add(cluster);
                }
                document.SetField("Clusters", clusters);
            }

            var clusterConfigLength = reader.ReadInt32EndianAware();

            byte[] clusterConfig = null;

            if (clusterConfigLength > 0) {
                clusterConfig = reader.ReadBytes(clusterConfigLength);
            }

            document.SetField("ClusterConfig", clusterConfig);

            var release = reader.ReadInt32PrefixedString();
            document.SetField("OrientdbRelease", release);

            return document;
        }
    }
}
