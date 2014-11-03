﻿using Orient.Client.API.Types;
using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;

// syntax: 
// UPDATE <class>|cluster:<cluster>> 
// SET|INCREMENT [= <field-value>](<field-name>)[<field-name> = <field-value>](,)* 
// [<conditions>] (WHERE) 
// [<max-records>](LIMIT)

// collections: 
// UPDATE <class>|cluster:<cluster>> 
// [[<field-name> = <field-value>](ADD|REMOVE])[<field-name> = <field-value>](,)* 
// [<conditions>](WHERE)

// maps:
// UPDATE <class>|cluster:<cluster>> 
// [[<field-name> = <map-key> [,<map-value>]](PUT|REMOVE])[<field-name> = <map-key> [,<map-value>]](,)* 
// [<conditions>](WHERE)
using Orient.Client.Protocol.Operations.Command;
using Orient.Client.Protocol.Query;

namespace Orient.Client.API.Query
{
    public class OSqlUpdate
    {
        private readonly SqlQuery _sqlQuery = new SqlQuery();
        private readonly Connection _connection;

        public OSqlUpdate()
        {
        }

        internal OSqlUpdate(Connection connection)
        {
            _connection = connection;
        }

        #region Update

        public OSqlUpdate Update(ORID orid)
        {
            _sqlQuery.Record(orid);

            return this;
        }

        public OSqlUpdate Update<T>(T obj)
        {
            _sqlQuery.Update(obj);

            return this;
        }

        #endregion

        #region Class

        public OSqlUpdate Class(string className)
        {
            _sqlQuery.Class(className);

            return this;
        }

        public OSqlUpdate Class<T>()
        {
            return Class(typeof(T).Name);
        }

        #endregion

        #region Cluster

        public OSqlUpdate Cluster(string clusterName)
        {
            _sqlQuery.Cluster("cluster:" + clusterName);

            return this;
        }

        public OSqlUpdate Cluster<T>()
        {
            return Cluster(typeof(T).Name);
        }

        #endregion

        #region Record

        public OSqlUpdate Record(ORID orid)
        {
            _sqlQuery.Record(orid);

            return this;
        }

        public OSqlUpdate Record(ODocument document)
        {
            return Record(document.ORID);
        }

        #endregion

        #region Set

        public OSqlUpdate Set<T>(string fieldName, T fieldValue)
        {
            _sqlQuery.Set<T>(fieldName, fieldValue);

            return this;
        }

        public OSqlUpdate Set<T>(T obj)
        {
            _sqlQuery.Set(obj);

            return this;
        }

        #endregion

        public OSqlUpdate Add<T>(string fieldName, T fieldValue)
        {
            _sqlQuery.Add(fieldName, fieldValue);

            return this;
        }

        #region Remove

        public OSqlUpdate Remove(string fieldName)
        {
            _sqlQuery.Remove(fieldName);

            return this;
        }

        public OSqlUpdate Remove<T>(string fieldName, T collectionValue)
        {
            _sqlQuery.Remove(fieldName, collectionValue);

            return this;
        }

        #endregion

        #region Where with conditions

        public OSqlUpdate Where(string field)
        {
            _sqlQuery.Where(field);

            return this;
        }

        public OSqlUpdate And(string field)
        {
            _sqlQuery.And(field);

            return this;
        }

        public OSqlUpdate Or(string field)
        {
            _sqlQuery.Or(field);

            return this;
        }

        public OSqlUpdate Equals<T>(T item)
        {
            _sqlQuery.Equals<T>(item);

            return this;
        }

        public OSqlUpdate NotEquals<T>(T item)
        {
            _sqlQuery.NotEquals<T>(item);

            return this;
        }

        public OSqlUpdate Lesser<T>(T item)
        {
            _sqlQuery.Lesser<T>(item);

            return this;
        }

        public OSqlUpdate LesserEqual<T>(T item)
        {
            _sqlQuery.LesserEqual<T>(item);

            return this;
        }

        public OSqlUpdate Greater<T>(T item)
        {
            _sqlQuery.Greater<T>(item);

            return this;
        }

        public OSqlUpdate GreaterEqual<T>(T item)
        {
            _sqlQuery.GreaterEqual<T>(item);

            return this;
        }

        public OSqlUpdate Like<T>(T item)
        {
            _sqlQuery.Like<T>(item);

            return this;
        }

        public OSqlUpdate IsNull()
        {
            _sqlQuery.IsNull();

            return this;
        }

        public OSqlUpdate Contains<T>(T item)
        {
            _sqlQuery.Contains<T>(item);

            return this;
        }

        public OSqlUpdate Contains<T>(string field, T value)
        {
            _sqlQuery.Contains<T>(field, value);

            return this;
        }

        #endregion

        public OSqlUpdate Limit(int maxRecords)
        {
            _sqlQuery.Limit(maxRecords);

            return this;
        }

        public int Run()
        {
            var payload = new CommandPayloadCommand {
                Text = ToString()
            };

            var operation = new Command {
                OperationMode = OperationMode.Synchronous,
                CommandPayload = payload
            };

            var result = new OCommandResult(_connection.ExecuteOperation(operation));

            return int.Parse(result.ToDocument().GetField<string>("Content"));
        }

        public override string ToString()
        {
            return _sqlQuery.ToString(QueryType.Update);
        }
    }
}
