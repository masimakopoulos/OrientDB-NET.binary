using System;
using System.Collections.Generic;
using System.Linq;
using Orient.Client.API;

namespace Orient.Client.Protocol
{
    internal class ConnectionPool
    {
        private readonly IServerAddress _serverAddress;
        public readonly IDatabaseConnectionInfo DatabaseConnectionInfo;
        private readonly Queue<Connection> _connections = new Queue<Connection>();
        internal string Release { get; private set; }

        internal int CurrentSize {
            get { return _connections.Count(con => con.IsActive); }
        }

        internal ConnectionPool(IServerAddress serverAddress,IDatabaseConnectionInfo databaseConnectionInfo) {
            if (serverAddress == null) throw new ArgumentNullException("serverAddress");
            if (databaseConnectionInfo == null) throw new ArgumentNullException("databaseConnectionInfo");

            _serverAddress = serverAddress;
            DatabaseConnectionInfo = databaseConnectionInfo;
            
            InitializeConnections();

            //get release from last connection
            var lastConnection = _connections.LastOrDefault();
            if (lastConnection != null) {
                Release = lastConnection.Document.GetField<string>("OrientdbRelease");
            }
        }

        private void InitializeConnections() {
            for (var i = 0; i < DatabaseConnectionInfo.PoolSize; i++) {
                var connection = new Connection(_serverAddress, DatabaseConnectionInfo, true);

                _connections.Enqueue(connection);
            }
        }

        internal void DropConnections() {
            foreach (var connection in _connections) {
                connection.Dispose();
            }
        }

        internal Connection DequeueConnection() {
            return _connections.Dequeue();
        }

        internal void EnqueueConnection(Connection connection) {
            _connections.Enqueue(connection);
        }
    }
}
