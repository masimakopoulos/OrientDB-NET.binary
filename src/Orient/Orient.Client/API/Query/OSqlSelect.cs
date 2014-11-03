using System.Collections.Generic;
using Orient.Client.API.Exceptions;
using Orient.Client.API.Types;
using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;

// syntax:
// SELECT [FROM <Target> 
// [LET <Assignment>*](<Projections>]) 
// [<Condition>*](WHERE) 
// [BY <Field>](GROUP) 
// [BY <Fields>* [ASC|DESC](ORDER)*] 
// [<SkipRecords>](SKIP) 
// [<MaxRecords>](LIMIT)
using Orient.Client.Protocol.Operations.Command;
using Orient.Client.Protocol.Query;

namespace Orient.Client.API.Query
{
    public class OSqlSelect
    {
        private readonly SqlQuery _sqlQuery = new SqlQuery();
        private readonly Connection _connection;

        public OSqlSelect() {
        }

        internal OSqlSelect(Connection connection) {
            _connection = connection;
        }

        #region Select

        public OSqlSelect Select(params string[] projections) {
            _sqlQuery.Select(projections);

            return this;
        }

        public OSqlSelect Also(string projection) {
            _sqlQuery.Also(projection);

            return this;
        }

        /*public OSqlSelect First()
        {
            _sqlQuery.Surround("first");

            return this;
        }*/

        public OSqlSelect Nth(int index) {
            _sqlQuery.Nth(index);

            return this;
        }

        public OSqlSelect As(string alias) {
            _sqlQuery.As(alias);

            return this;
        }

        #endregion

        #region From

        public OSqlSelect From(string target) {
            _sqlQuery.From(target);

            return this;
        }

        public OSqlSelect From(ORID orid) {
            _sqlQuery.From(orid);

            return this;
        }

        public OSqlSelect From(ODocument document) {
            if ((document.ORID == null) && string.IsNullOrEmpty(document.OClassName)) {
                throw new OException(OExceptionType.Query, "Document doesn't contain ORID or OClassName value.");
            }

            _sqlQuery.From(document);

            return this;
        }

        public OSqlSelect From<T>() {
            return From(typeof (T).Name);
        }

        #endregion

        #region Where with conditions

        public OSqlSelect Where(string field) {
            _sqlQuery.Where(field);

            return this;
        }

        public OSqlSelect And(string field) {
            _sqlQuery.And(field);

            return this;
        }

        public OSqlSelect Or(string field) {
            _sqlQuery.Or(field);

            return this;
        }

        public OSqlSelect Equals<T>(T item) {
            _sqlQuery.Equals(item);

            return this;
        }

        public OSqlSelect NotEquals<T>(T item) {
            _sqlQuery.NotEquals(item);

            return this;
        }

        public OSqlSelect Lesser<T>(T item) {
            _sqlQuery.Lesser(item);

            return this;
        }

        public OSqlSelect LesserEqual<T>(T item) {
            _sqlQuery.LesserEqual(item);

            return this;
        }

        public OSqlSelect Greater<T>(T item) {
            _sqlQuery.Greater(item);

            return this;
        }

        public OSqlSelect GreaterEqual<T>(T item) {
            _sqlQuery.GreaterEqual(item);

            return this;
        }

        public OSqlSelect Like<T>(T item) {
            _sqlQuery.Like(item);

            return this;
        }

        public OSqlSelect IsNull() {
            _sqlQuery.IsNull();

            return this;
        }

        public OSqlSelect Contains<T>(T item) {
            _sqlQuery.Contains(item);

            return this;
        }

        public OSqlSelect Contains<T>(string field, T value) {
            _sqlQuery.Contains(field, value);

            return this;
        }

        #endregion

        public OSqlSelect OrderBy(params string[] fields) {
            _sqlQuery.OrderBy(fields);

            return this;
        }

        public OSqlSelect Ascending() {
            _sqlQuery.Ascending();

            return this;
        }

        public OSqlSelect Descending() {
            _sqlQuery.Descending();

            return this;
        }

        public OSqlSelect Skip(int skipCount) {
            _sqlQuery.Skip(skipCount);

            return this;
        }

        public OSqlSelect Limit(int maxRecords) {
            _sqlQuery.Limit(maxRecords);

            return this;
        }

        #region ToList

        public List<T> ToList<T>() where T : class, new() {
            var result = new List<T>();
            var documents = ToList("*:0");

            foreach (var document in documents) {
                result.Add(document.To<T>());
            }

            return UpdateFromTransaction(result);
        }

        public List<ODocument> ToList() {
            return ToList("*:0");
        }

        public List<ODocument> ToList(string fetchPlan) {
            var payload = new CommandPayloadQuery {
                Text = ToString(),
                NonTextLimit = -1,
                FetchPlan = fetchPlan,
                //SerializedParams = new byte[] {0}
            };

            var operation = new Command {
                OperationMode = OperationMode.Asynchronous,
                CommandPayload = payload
            };

            var commandResult = new OCommandResult(_connection.ExecuteOperation(operation));

            return commandResult.ToList();
        }

        private List<T> UpdateFromTransaction<T>(IEnumerable<T> recordsFromDb) where T : class {
            var result = new List<T>();

            foreach (var record in recordsFromDb) {
                if (record is IBaseRecord) {
                    var baseRecord = (IBaseRecord) record;
                    var cachedRecord = _connection.Database.Transaction.GetPendingObject<IBaseRecord>(baseRecord.ORID);
                    result.Add(cachedRecord as T ?? record);
                }
                else {
                    result.Add(record);
                }
            }

            return result;
        }

        #endregion

        public override string ToString() {
            return _sqlQuery.ToString(QueryType.Select);
        }
    }
}