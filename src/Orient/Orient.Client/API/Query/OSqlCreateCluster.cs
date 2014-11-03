using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;

// syntax:
// CREATE CLUSTER <name> <type> 
// [DATASEGMENT <data-segment>|default] 
// [LOCATION <path>|default] 
// [POSITION <position>|append]
using Orient.Client.Protocol.Operations.Command;
using Orient.Client.Protocol.Query;

namespace Orient.Client.API.Query
{

    public interface OSqlCreateCluster {
        OSqlCreateCluster Cluster(string clusterName, OClusterType clusterType);
        OSqlCreateCluster Cluster<T>(OClusterType clusterType);
        short Run();
        string ToString();
    }

    public class OSqlCreateClusterViaSql : OSqlCreateCluster
    {
        private readonly SqlQuery _sqlQuery = new SqlQuery();
        private readonly Connection _connection;

        public OSqlCreateClusterViaSql() {
        }

        internal OSqlCreateClusterViaSql(Connection connection) {
            _connection = connection;
        }

        #region Cluster

        public OSqlCreateCluster Cluster(string clusterName, OClusterType clusterType) {
            _sqlQuery.Cluster(clusterName, clusterType);

            return this;
        }

        public OSqlCreateCluster Cluster<T>(OClusterType clusterType) {
            return Cluster(typeof(T).Name, clusterType);
        }

        #endregion

        public short Run() {
            var payload = new CommandPayloadCommand() {
                Text = ToString()
            };

            var operation = new Command {
                OperationMode = OperationMode.Synchronous,
                CommandPayload = payload
            };

            var result = new OCommandResult(_connection.ExecuteOperation(operation));

            return short.Parse(result.ToDocument().GetField<string>("Content"));
        }

        public override string ToString() {
            return _sqlQuery.ToString(QueryType.CreateCluster);
        }
    }
}
