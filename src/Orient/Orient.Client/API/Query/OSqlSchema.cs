using System.Collections.Generic;
using System.Linq;
using Orient.Client.API.Types;
using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;
using Orient.Client.Protocol.Operations.Command;

namespace Orient.Client.API.Query
{
    public class OSqlSchema
    {
        private readonly Connection _connection;
        private const string Query = "select expand(classes) from metadata:schema";
        private readonly IEnumerable<ODocument> _schema;

        internal OSqlSchema(Connection connection) {
            _connection = connection;
            _schema = Run();
        }

        public IEnumerable<string> Classes() {
            return _schema.Select(c => c.GetField<string>("name"));
        }

        public string SuperClass(string @class) {
            return _schema.First(c => c.GetField<string>("name") == @class).GetField<string>("superClass");
        }

        public IEnumerable<ODocument> Properties(string @class) {
            var pDocument = _schema.FirstOrDefault(d => d.GetField<string>("name") == @class);
            return pDocument != null ? pDocument.GetField<HashSet<ODocument>>("properties") : null;
        }

        public IEnumerable<ODocument> Properties<T>() {
            var @class = typeof (T).Name;
            return Properties(@class);
        }

        private IEnumerable<ODocument> Run() {
            var payload = new CommandPayloadCommand() {
                Text = Query,
            };

            var operation = new Command {
                OperationMode = OperationMode.Asynchronous,
                CommandPayload = payload
            };

            var document = _connection.ExecuteOperation(operation);

            return document.GetField<List<ODocument>>("Content");

        }
    }
}