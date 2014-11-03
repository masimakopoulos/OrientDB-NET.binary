using System.Linq;
using Orient.Client.API.Exceptions;
using Orient.Client.API.Types;
using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;

namespace Orient.Client.API.Query
{
    public class OCreateEdgeRecord : IOSqlCreateEdge
    {
        private readonly Connection _connection;
        private ODocument _document;
        private ORID _source;
        private ORID _dest;
        private string _edgeName;

        public OCreateEdgeRecord() {
        }

        internal OCreateEdgeRecord(Connection connection) {
            _connection = connection;
        }

        #region Edge

        public IOSqlCreateEdge Edge(string className) {
            _edgeName = className;

            return this;
        }

        public IOSqlCreateEdge Edge<T>(T obj) {

            if (obj is ODocument) {
                _document = obj as ODocument;
            } else {
                _document = ODocument.ToDocument(obj);
            }

            if (string.IsNullOrEmpty(_document.OClassName)) {
                throw new OException(OExceptionType.Query, "Document doesn't contain OClassName value.");
            }

            return this;
        }

        public IOSqlCreateEdge Edge<T>() {
            return Edge(typeof(T).Name);
        }

        #endregion

        #region Cluster

        public IOSqlCreateEdge Cluster(string clusterName) {
            if (_document.ORID == null)
                _document.ORID = new ORID();

            _document.ORID.ClusterId = _connection.Database.GetClusters().First(x => x.Name == clusterName).Id;

            return this;
        }

        public IOSqlCreateEdge Cluster<T>() {
            return Cluster(typeof(T).Name);
        }

        #endregion

        #region Set

        public IOSqlCreateEdge Set<T>(string fieldName, T fieldValue) {
            if (_document == null)
                _document = new ODocument();
            _document.SetField(fieldName, fieldValue);

            return this;
        }

        public IOSqlCreateEdge Set<T>(T obj) {
            var document = obj is ODocument ? obj as ODocument : ODocument.ToDocument(obj);

            // TODO: go also through embedded fields
            foreach (var field in document) {
                // set only fields which doesn't start with @ character
                if ((field.Key.Length > 0) && (field.Key[0] != '@')) {
                    Set(field.Key, field.Value);
                }
            }

            return this;
        }

        #endregion



        public OEdge Run() {
            if (_document == null) {
                // simple link, no properties?
            }

            //            var operation = CreateSQLOperation();

            var operation = new CreateRecord(_document, _connection.Database);

            return _connection.ExecuteOperation(operation).To<OEdge>();
        }



        public T Run<T>() where T : class, new() {
            return Run().To<T>();
        }


        public IOSqlCreateEdge From(ORID orid) {
            _source = orid;
            return this;
        }

        public IOSqlCreateEdge From<T>(T obj) {
            _source = ToODocument(obj).ORID;
            return this;

        }

        public IOSqlCreateEdge To(ORID orid) {
            _dest = orid;
            return this;
        }

        public IOSqlCreateEdge To<T>(T obj) {
            _dest = ToODocument(obj).ORID;
            return this;
        }

        private static ODocument ToODocument<T>(T obj) {
            ODocument document;

            if (obj is ODocument) {
                document = obj as ODocument;
            } else {
                document = ODocument.ToDocument(obj);
            }

            if (document.ORID == null) {
                throw new OException(OExceptionType.Query, "Document doesn't contain ORID value.");
            }
            return document;
        }
    }
}
