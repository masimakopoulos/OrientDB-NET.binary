using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;

namespace Orient.Client.API.Query
{
    public class OSqlDeleteCluster
    {
        private readonly Connection _connection;
        private readonly short _clusterid;

        public OSqlDeleteCluster() {

        }

        internal OSqlDeleteCluster(Connection connection, short clusterid) {
            _connection = connection;
            _clusterid = clusterid;
        }

        public bool Run() {
            var operation = new DataClusterDrop();
            operation.ClusterId = _clusterid;
            var document = _connection.ExecuteOperation(operation);
            var result = document.GetField<bool>("remove_localy");
            if (result)
                _connection.Database.RemoveCluster(_clusterid);
            return result;
        }
    }
}