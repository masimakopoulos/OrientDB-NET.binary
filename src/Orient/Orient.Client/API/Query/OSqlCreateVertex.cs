﻿using Orient.Client.API.Exceptions;
using Orient.Client.API.Types;
using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;

// syntax: 
// CREATE VERTEX [<class>] 
// [CLUSTER <cluster>] 
// [SET <field> = <expression>[,]*]
using Orient.Client.Protocol.Operations.Command;
using Orient.Client.Protocol.Query;

namespace Orient.Client.API.Query
{
    public interface OSqlCreateVertex
    {
        OSqlCreateVertex Vertex(string className);
        OSqlCreateVertex Vertex<T>(T obj);
        OSqlCreateVertex Vertex<T>();
        OSqlCreateVertex Cluster(string clusterName);
        OSqlCreateVertex Cluster<T>();
        OSqlCreateVertex Set<T>(string fieldName, T fieldValue);
        OSqlCreateVertex Set<T>(T obj);
        OVertex Run();
        T Run<T>() where T : class, new();
        string ToString();
    }

    public class OSqlCreateVertexViaSql : OSqlCreateVertex
    {
        private readonly SqlQuery _sqlQuery = new SqlQuery();
        private readonly Connection _connection;

        public OSqlCreateVertexViaSql()
        {
        }

        internal OSqlCreateVertexViaSql(Connection connection)
        {
            _connection = connection;
        }

        #region Vertex

        public OSqlCreateVertex Vertex(string className)
        {
            _sqlQuery.Vertex(className);

            return this;
        }

        public OSqlCreateVertex Vertex<T>(T obj)
        {
            ODocument document;

            if (obj is ODocument)
            {
                document = obj as ODocument;
            }
            else
            {
                document = ODocument.ToDocument(obj);
            }

            if (string.IsNullOrEmpty(document.OClassName))
            {
                throw new OException(OExceptionType.Query, "Document doesn't contain OClassName value.");
            }

            _sqlQuery.Vertex(document.OClassName);
            _sqlQuery.Set(document);

            return this;
        }

        public OSqlCreateVertex Vertex<T>()
        {
            return Vertex(typeof(T).Name);
        }

        #endregion

        #region Cluster

        public OSqlCreateVertex Cluster(string clusterName)
        {
            _sqlQuery.Cluster(clusterName);

            return this;
        }

        public OSqlCreateVertex Cluster<T>()
        {
            return Cluster(typeof(T).Name);
        }

        #endregion

        #region Set

        public OSqlCreateVertex Set<T>(string fieldName, T fieldValue)
        {
            _sqlQuery.Set<T>(fieldName, fieldValue);

            return this;
        }

        public OSqlCreateVertex Set<T>(T obj)
        {
            _sqlQuery.Set(obj);

            return this;
        }

        #endregion

        #region Run

        public OVertex Run()
        {
            var payload = new CommandPayloadCommand {Text = ToString()};

            var operation = new Command {
                OperationMode = OperationMode.Synchronous,
                CommandPayload = payload
            };

            var result = new OCommandResult(_connection.ExecuteOperation(operation));

            return result.ToSingle().To<OVertex>();
        }

        public T Run<T>() where T : class, new()
        {
            return Run().To<T>();
        }

        #endregion

        public override string ToString()
        {
            return _sqlQuery.ToString(QueryType.CreateVertex);
        }
    }
}
