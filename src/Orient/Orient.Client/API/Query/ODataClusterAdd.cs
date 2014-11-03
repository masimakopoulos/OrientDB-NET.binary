using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;

namespace Orient.Client.API.Query
{
    public class ODataClusterAdd : OSqlCreateCluster
    {
        private readonly Connection _connection;
        public string ClusterName { get; set; }
        public OClusterType ClusterType { get; set; }

        public ODataClusterAdd() {

        }

        internal ODataClusterAdd(Connection connection) {
            _connection = connection;
        }

        public OSqlCreateCluster Cluster(string clusterName, OClusterType clusterType) {
            ClusterName = clusterName;
            ClusterType = clusterType;
            return this;
        }

        public OSqlCreateCluster Cluster<T>(OClusterType clusterType) {
            return Cluster(typeof (T).Name, clusterType);
        }

        public short Run() {
           var operation = new DataClusterAdd {ClusterType = ClusterType, ClusterName = ClusterName};
            var document = _connection.ExecuteOperation(operation);
            var clusterid = document.GetField<short>("ClusterId");
            if (clusterid != -1)
                _connection.Database.AddCluster(new OCluster { Name = ClusterName, Id = clusterid, Type = ClusterType });
            return clusterid;

        }
    }
}