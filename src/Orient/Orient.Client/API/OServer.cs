using System;
using System.Collections.Generic;
using Orient.Client.API.Types;
using Orient.Client.Protocol;
using Orient.Client.Protocol.Operations;

namespace Orient.Client.API
{
    public class OServer : IDisposable
    {
        private readonly Connection _connection;

        public OServer(IServerAddress serverAddress, IServerCredentials serverCredentials) {
            if (serverAddress == null) throw new ArgumentNullException("serverAddress");
            if (serverCredentials == null) throw new ArgumentNullException("serverCredentials");

            _connection = new Connection(serverAddress, serverCredentials);
        }

        public bool CreateDatabase(string databaseName, ODatabaseType databaseType, OStorageType storageType) {
            var operation = new DbCreate {
                DatabaseName = databaseName,
                DatabaseType = databaseType,
                StorageType = storageType
            };

            var document = _connection.ExecuteOperation(operation);

            return document.GetField<bool>("IsCreated");
        }

        public bool DatabaseExists(string databaseName, OStorageType storageType) {
            var operation = new DbExist {DatabaseName = databaseName, StorageType = storageType};

            var document = _connection.ExecuteOperation(operation);

            return document.GetField<bool>("Exists");
        }

        public void DropDatabase(string databaseName, OStorageType storageType) {
            var operation = new DbDrop {DatabaseName = databaseName, StorageType = storageType};

            _connection.ExecuteOperation(operation);
        }


        #region Configuration

        public string ConfigGet(string key) {
            var operation = new ConfigGet {ConfigKey = key};
            var document = _connection.ExecuteOperation(operation);
            return document.GetField<string>(key);
        }

        public bool ConfigSet(string configKey, string configValue) {
            var operation = new ConfigSet {Key = configKey, Value = configValue};
            var document = _connection.ExecuteOperation(operation);

            return document.GetField<bool>("IsCreated");
        }

        public Dictionary<string, string> ConfigList() {
            var operation = new ConfigList();
            var document = _connection.ExecuteOperation(operation);
            return document.GetField<Dictionary<string, string>>("config");
        }

        #endregion

        public Dictionary<string, string> Databases() {
            var returnValue = new Dictionary<string, string>();
            var operation = new DBList();
            var document = _connection.ExecuteOperation(operation);
            var databases = document.GetField<string>("databases").Split(',');
            foreach (var item in databases) {
                if(string.IsNullOrEmpty(item)) continue;
                var keyValue = item.Split(':');
                returnValue.Add(keyValue[0], keyValue[1] + ":" + keyValue[2]);
            }
            return returnValue;
        }

        public void Close() {
            _connection.Dispose();
        }

        public void Dispose() {
            Close();
        }
    }
}