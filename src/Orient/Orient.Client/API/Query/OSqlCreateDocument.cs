using Orient.Client.API.Types;
using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;

// shorthand for INSERT INTO for documents
using Orient.Client.Protocol.Operations.Command;
using Orient.Client.Protocol.Query;

namespace Orient.Client.API.Query
{
    public class OSqlCreateDocument
    {
        private readonly SqlQuery _sqlQuery = new SqlQuery();
        private readonly Connection _connection;

        public OSqlCreateDocument() {
        }

        internal OSqlCreateDocument(Connection connection) {
            _connection = connection;
        }

        #region Document

        public OSqlCreateDocument Document(string className) {
            _sqlQuery.Class(className);

            return this;
        }

        public OSqlCreateDocument Document<T>(T obj) {
            // check for OClassName shouldn't have be here since INTO clause might specify it

            _sqlQuery.Insert(obj);

            return this;
        }

        public OSqlCreateDocument Document<T>() {
            return Document(typeof(T).Name);
        }

        #endregion

        #region Cluster

        public OSqlCreateDocument Cluster(string clusterName) {
            _sqlQuery.Cluster(clusterName);

            return this;
        }

        public OSqlCreateDocument Cluster<T>() {
            return Cluster(typeof(T).Name);
        }

        #endregion

        #region Set

        public OSqlCreateDocument Set<T>(string fieldName, T fieldValue) {
            _sqlQuery.Set<T>(fieldName, fieldValue);

            return this;
        }

        public OSqlCreateDocument Set<T>(T obj) {
            _sqlQuery.Set(obj);

            return this;
        }

        #endregion

        #region Run

        public ODocument Run() {
            var payload = new CommandPayloadCommand {Text = ToString()};

            var operation = new Command {
                OperationMode = OperationMode.Synchronous,
                CommandPayload = payload
            };

            var result = new OCommandResult(_connection.ExecuteOperation(operation));

            return result.ToSingle();
        }

        public T Run<T>() where T : class, new() {
            return Run().To<T>();
        }

        #endregion

        public override string ToString() {
            return _sqlQuery.ToString(QueryType.Insert);
        }
    }
}
