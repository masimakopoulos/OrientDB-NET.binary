using Orient.Client.API.Types;
using Orient.Client.Protocol;

namespace Orient.Client.API.Query
{
    public class OSqlCreate
    {
        private readonly Connection _connection;

        internal OSqlCreate(Connection connection) {
            _connection = connection;
        }

        #region Class

        public OSqlCreateClass Class(string className) {
            return new OSqlCreateClass(_connection).Class(className);
        }

        public OSqlCreateProperty Property(string propertyName, OType type) {
            return new OSqlCreateProperty(_connection).Property(propertyName, type);
        }

        public OSqlCreateClass Class<T>() {
            return new OSqlCreateClass(_connection).Class<T>();
        }

        #endregion

        #region Cluster

        public OSqlCreateCluster Cluster(string clusterName, OClusterType clusterType) {
            //return new OSqlCreateClusterViaSql(_connection).Cluster(clusterName, clusterType);
            return new ODataClusterAdd(_connection).Cluster(clusterName, clusterType);
        }

        public OSqlCreateCluster Cluster<T>(OClusterType clusterType) {
            return Cluster(typeof (T).Name, clusterType);
        }

        #endregion

        #region Document

        public OSqlCreateDocument Document(string className) {
            return new OSqlCreateDocument(_connection)
                .Document(className);
        }

        public OSqlCreateDocument Document<T>() {
            return Document(typeof (T).Name);
        }

        public OSqlCreateDocument Document<T>(T obj) {
            return new OSqlCreateDocument(_connection)
                .Document(obj);
        }

        #endregion

        #region Vertex

        public OSqlCreateVertex Vertex(string className) {
            return new OCreateVertexRecord(_connection)
                .Vertex(className);
        }

        public OSqlCreateVertex Vertex<T>() {
            return Vertex(typeof (T).Name);
        }

        public OSqlCreateVertex Vertex<T>(T obj) {
            return new OCreateVertexRecord(_connection)
                .Vertex(obj);
        }

        #endregion

        #region Edge

        public IOSqlCreateEdge FlexibleEdge(string className) {
            return new ΟSqlCreateEdgeViaSql(_connection)
                .Edge(className);
        }

        public IOSqlCreateEdge FlexibleEdge<T>() {
            return FlexibleEdge(typeof (T).Name);
        }

        public IOSqlCreateEdge FlexibleEdge<T>(T obj) {
            return new ΟSqlCreateEdgeViaSql(_connection)
                .Edge(obj);
        }

        public IOSqlCreateGenericEdge<TFrom, TTo> Edge<TFrom, TTo>(GenericEdge<TFrom, TTo> obj) {
            return new OSqlCreateGenericEdge<TFrom, TTo>(_connection).Edge(obj);
        }

        #endregion
    }
}