using System;
using System.Collections.Generic;
using System.Linq;
using Orient.Client.API.Query;
using Orient.Client.API.Types;
using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;
using Orient.Client.Protocol.Operations.Command;

namespace Orient.Client.API
{
    public class ODatabase : IODatabase, IDisposable
    {
        private readonly IConnectionManager _connectionManager;
        private readonly Connection _connection;
        private readonly Guid _id;

        public string Name { get; private set; }
        public IDictionary<ORID, ODocument> ClientCache { get; private set; }

        public OSqlCreate Create {
            get { return new OSqlCreate(_connection); }
        }

        public OSqlDelete Delete {
            get { return new OSqlDelete(_connection); }
        }

        public OLoadRecord Load {
            get { return new OLoadRecord(_connection); }
        }

        public ORecordMetadata Metadata {
            get { return new ORecordMetadata(_connection); }
        }

        public OSqlSchema Schema {
            get { return new OSqlSchema(_connection); }
        }

        public OTransaction Transaction { get; private set; }

        public Guid Id {
            get { return _id; }
        }

        public ODatabase(IConnectionManager connectionManager, IDatabaseConnectionInfo databaseConnectionInfo,
            Guid contextId) {
            if (connectionManager == null) throw new ArgumentNullException("connectionManager");
            if (databaseConnectionInfo == null) throw new ArgumentNullException("databaseConnectionInfo");

            _connectionManager = connectionManager;
            Name = databaseConnectionInfo.DatabaseName;

            _connection = _connectionManager.Get(databaseConnectionInfo);
            _connection.Database = this;
            ClientCache = new Dictionary<ORID, ODocument>();
            Transaction = new OTransaction(_connection);
            _id = contextId;
        }

        public IEnumerable<OCluster> GetClusters() {
            return _connection.Document.GetField<List<OCluster>>("Clusters");
        }

        public short GetClusterIdFor(string className) {
            var clusterName = CorrectClassName(className).ToLower();
            var oCluster = GetClusters().FirstOrDefault(x => x.Name == clusterName);
            if (oCluster != null) return oCluster.Id;
            _connection.Reload();
            oCluster = GetClusters().First(x => x.Name == clusterName);
            return oCluster.Id;
        }

        public string GetClusterNameFor(short clusterId) {
            var oCluster = GetClusters().FirstOrDefault(x => x.Id == clusterId);
            if (oCluster != null) return oCluster.Name;
            _connection.Reload();
            oCluster = GetClusters().FirstOrDefault(x => x.Id == clusterId);
            return oCluster != null ? oCluster.Name : null;
        }

        private string CorrectClassName(string className) {
            if (className == "OVertex")
                return "V";
            if (className == "OEdge")
                return "E";
            return className;
        }

        internal void AddCluster(OCluster cluster) {
            var clusters = _connection.Document.GetField<List<OCluster>>("Clusters");
            clusters.Add(cluster);

        }

        internal void RemoveCluster(short clusterid) {
            var clusters = _connection.Document.GetField<List<OCluster>>("Clusters");
            var cluster = clusters.SingleOrDefault(c => c.Id == clusterid);
            if (cluster != null) clusters.Remove(cluster);
        }

        public OSqlSelect Select(params string[] projections) {
            return new OSqlSelect(_connection).Select(projections);
        }

        public OClusterQuery Clusters(params string[] clusterNames) {
            return Clusters(clusterNames.Select(n => new OCluster {Name = n, Id = GetClusterIdFor(n)}));
        }

        private OClusterQuery Clusters(IEnumerable<OCluster> clusters) {
            var query = new OClusterQuery(_connection);
            foreach (var id in clusters) {
                query.AddClusterId(id);
            }
            return query;
        }

        public OClusterQuery Clusters(params short[] clusterIds) {
            return Clusters(clusterIds.Select(id => new OCluster {Id = id}));
        }

        #region Insert

        public OSqlInsert Insert() {
            return new OSqlInsert(_connection);
        }

        public OSqlInsert Insert<T>(T obj) {
            return new OSqlInsert(_connection)
                .Insert(obj);
        }

        #endregion

        #region Update

        public OSqlUpdate Update() {
            return new OSqlUpdate(_connection);
        }

        public OSqlUpdate Update(ORID orid) {
            return new OSqlUpdate(_connection)
                .Update(orid);
        }

        public OSqlUpdate Update<T>(T obj) {
            return new OSqlUpdate(_connection)
                .Update(obj);
        }

        #endregion

        #region Query

        public List<ODocument> Query(string sql) {
            return Query(sql, "*:0");
        }

        public List<ODocument> Query(string sql, string fetchPlan) {
            var payload = new CommandPayloadQuery {
                Text = sql,
                NonTextLimit = -1,
                FetchPlan = fetchPlan
            };

            var operation = new Command {
                OperationMode = OperationMode.Asynchronous,
                CommandPayload = payload
            };

            var document = _connection.ExecuteOperation(operation);

            return document.GetField<List<ODocument>>("Content");
        }

        #endregion

        public List<ODocument> Gremlin(string query) {
            var payload = new CommandPayloadScript {Language = "gremlin", Text = query};
            var operation = new Command {OperationMode = OperationMode.Synchronous, CommandPayload = payload};
            var document = _connection.ExecuteOperation(operation);
            return document.GetField<List<ODocument>>("Content");
        }

        public List<ODocument> JavaScript(string query) {
            var payload = new CommandPayloadScript {Language = "javascript", Text = query};
            var operation = new Command {OperationMode = OperationMode.Synchronous, CommandPayload = payload};
            var document = _connection.ExecuteOperation(operation);
            return document.GetField<List<ODocument>>("Content");
        }

        public OCommandResult Command(string sql) {
            var payload = new CommandPayloadCommand {Text = sql};
            var operation = new Command {OperationMode = OperationMode.Synchronous, CommandPayload = payload};
            var document = _connection.ExecuteOperation(operation);
            return new OCommandResult(document);
        }

        public void SaveChanges() {
            Transaction.Commit();
        }

        public long Size {
            get {
                var operation = new DbSize();
                var document = _connection.ExecuteOperation(operation);
                return document.GetField<long>("Size");
            }
        }

        public long CountRecords {
            get {
                var operation = new DbCountRecords();
                var document = _connection.ExecuteOperation(operation);
                return document.GetField<long>("Count");
            }
        }

        public void Dispose() {
            _connectionManager.Release(_connection);
        }
    }
}