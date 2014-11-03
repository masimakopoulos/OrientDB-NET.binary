using System;
using Orient.Client.API.Exceptions;
using Orient.Client.API.Types;
using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;

// syntax: 
// CREATE EDGE [<class>] 
// [CLUSTER <cluster>] 
// FROM <rid>|(<query>)|[<rid>]* 
// TO <rid>|(<query>)|[<rid>]* 
// [SET <field> = <expression>[,]*]
using Orient.Client.Protocol.Operations.Command;
using Orient.Client.Protocol.Query;

namespace Orient.Client.API.Query
{
    public interface IOSqlCreateEdge
    {
        IOSqlCreateEdge Edge(string className);
        IOSqlCreateEdge Edge<T>(T obj);
        IOSqlCreateEdge Edge<T>();
        IOSqlCreateEdge Cluster(string clusterName);
        IOSqlCreateEdge Cluster<T>();
        IOSqlCreateEdge From(ORID orid);
        IOSqlCreateEdge From<T>(T obj);
        IOSqlCreateEdge To(ORID orid);
        IOSqlCreateEdge To<T>(T obj);
        IOSqlCreateEdge Set<T>(string fieldName, T fieldValue);
        IOSqlCreateEdge Set<T>(T obj);
        OEdge Run();
        T Run<T>() where T : class, new();
        string ToString();
    }

    public class ΟSqlCreateEdgeViaSql : IOSqlCreateEdge
    {
        private readonly SqlQuery _sqlQuery = new SqlQuery();
        private readonly Connection _connection;

        public ΟSqlCreateEdgeViaSql() {
        }

        internal ΟSqlCreateEdgeViaSql(Connection connection) {
            _connection = connection;
        }

        #region Edge

        public IOSqlCreateEdge Edge(string className) {
            _sqlQuery.Edge(className);

            return this;
        }

        public IOSqlCreateEdge Edge<T>(T obj) {
            ODocument document;

            if (obj is ODocument) {
                document = obj as ODocument;
            }
            else {
                document = ODocument.ToDocument(obj);
            }

            var className = document.OClassName;
            
            if (String.IsNullOrEmpty(document.OClassName)) {
                throw new OException(OExceptionType.Query, "Document doesn't contain OClassName value.");
            }

            _sqlQuery.Edge(className);
            _sqlQuery.Set(document);

            return this;
        }

        public IOSqlCreateEdge Edge<T>() {
            return Edge(typeof (T).Name);
        }

        #endregion

        #region Cluster

        public IOSqlCreateEdge Cluster(string clusterName) {
            _sqlQuery.Cluster(clusterName);

            return this;
        }

        public IOSqlCreateEdge Cluster<T>() {
            return Cluster(typeof (T).Name);
        }

        #endregion

        #region From

        public IOSqlCreateEdge From(ORID orid) {
            _sqlQuery.From(orid);

            return this;
        }

        public IOSqlCreateEdge From<T>(T obj) {
            ODocument document;

            if (obj is ODocument) {
                document = obj as ODocument;
            }
            else {
                document = ODocument.ToDocument(obj);
            }

            if (document.ORID == null) {
                throw new OException(OExceptionType.Query, "Document doesn't contain ORID value.");
            }

            _sqlQuery.From(document.ORID);

            return this;
        }

        #endregion

        #region To

        public IOSqlCreateEdge To(ORID orid) {
            _sqlQuery.To(orid);

            return this;
        }

        public IOSqlCreateEdge To<T>(T obj) {
            ODocument document;

            if (obj is ODocument) {
                document = obj as ODocument;
            }
            else {
                document = ODocument.ToDocument(obj);
            }

            if (document.ORID == null) {
                throw new OException(OExceptionType.Query, "Document doesn't contain ORID value.");
            }

            _sqlQuery.To(document.ORID);

            return this;
        }

        #endregion

        #region Set

        public IOSqlCreateEdge Set<T>(string fieldName, T fieldValue) {
            _sqlQuery.Set(fieldName, fieldValue);

            return this;
        }

        public IOSqlCreateEdge Set<T>(T obj) {
            _sqlQuery.Set(obj);

            return this;
        }

        #endregion

        #region Run

        public OEdge Run() {
            var payload = new CommandPayloadCommand() {
                Text = ToString()
            };

            var operation = new Command {
                OperationMode = OperationMode.Synchronous,
                CommandPayload = payload
            };

            var result = new OCommandResult(_connection.ExecuteOperation(operation));

            return result.ToSingle().To<OEdge>();
        }

        public T Run<T>() where T : class, new() {
            return Run().To<T>();
        }

        #endregion

        public override string ToString() {
            return _sqlQuery.ToString(QueryType.CreateEdge);
        }
    }
}
