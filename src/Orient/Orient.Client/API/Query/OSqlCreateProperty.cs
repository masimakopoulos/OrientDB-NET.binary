using Orient.Client.API.Exceptions;
using Orient.Client.API.Types;
using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;
using Orient.Client.Protocol.Operations.Command;
using Orient.Client.Protocol.Query;

namespace Orient.Client.API.Query
{
    public class OSqlCreateProperty
    {
        private readonly SqlQuery _sqlQuery = new SqlQuery();
        private readonly Connection _connection;
        private string _propertyName;
        private string _class;
        private OType _type;

        public OSqlCreateProperty() {
        }

        internal OSqlCreateProperty(Connection connection) {
            _connection = connection;
        }

        public OSqlCreateProperty Property(string propertyName, OType type) {
            _propertyName = propertyName;
            _type = type;
            _sqlQuery.Property(_propertyName, _type);
            return this;
        }

        public short Run() {
            if (string.IsNullOrEmpty(_class))
                throw new OException(OExceptionType.Query, "Class is empty");

            var payload = new CommandPayloadCommand {
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
            return _sqlQuery.ToString(QueryType.CreateProperty);
        }

        public OSqlCreateProperty Class(string @class) {
            _class = @class;
            _sqlQuery.Class(_class);
            return this;
        }

        public OSqlCreateProperty LinkedType(OType type) {
            _sqlQuery.LinkedType(type);
            return this;
        }

        public OSqlCreateProperty LinkedClass(string @class) {
            _sqlQuery.LinkedClass(@class);
            return this;
        }
    }
}