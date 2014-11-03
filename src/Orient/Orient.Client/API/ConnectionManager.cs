using System;
using System.Collections.Generic;
using System.Linq;
using Orient.Client.Protocol;

namespace Orient.Client.API
{
    public class ConnectionManager : IConnectionManager, IDisposable
    {
        private readonly IServerAddress _serverAddress;
        private readonly object _syncRoot;
        private readonly List<ConnectionPool> _connectionPools;

        public ConnectionManager(IServerAddress serverAddress) {
            if (serverAddress == null) throw new ArgumentNullException("serverAddress");

            _serverAddress = serverAddress;
            _syncRoot = new object();
            _connectionPools = new List<ConnectionPool>();
        }

        public Connection Get(IDatabaseConnectionInfo databaseConnectionInfo) {
            if (databaseConnectionInfo == null) throw new ArgumentNullException("databaseConnectionInfo");

            lock (_syncRoot) {
                var connectionPool = _connectionPools.SingleOrDefault(
                    pool => pool.DatabaseConnectionInfo.DatabaseName == databaseConnectionInfo.DatabaseName)
                                   ?? CreateConnectionPool(databaseConnectionInfo);

                return connectionPool.CurrentSize > 0
                    ? connectionPool.DequeueConnection()
                    : new Connection(_serverAddress, databaseConnectionInfo, true);
            }
        }

        private ConnectionPool CreateConnectionPool(IDatabaseConnectionInfo databaseConnectionInfo) {
            lock (_syncRoot) {
                var connectionPool = new ConnectionPool(_serverAddress, databaseConnectionInfo);

                _connectionPools.Add(connectionPool);
                return connectionPool;
            }
        }

        public void Release(Connection connection) {
            lock (_syncRoot) {
                var connectionPool = _connectionPools.SingleOrDefault(
                    pool => pool.DatabaseConnectionInfo.DatabaseName == connection.DatabaseName);

                if ((connectionPool != null) &&
                    (connectionPool.CurrentSize < connectionPool.DatabaseConnectionInfo.PoolSize) &&
                    connection.IsActive &&
                    connection.IsReusable) {
                    connectionPool.EnqueueConnection(connection);
                } else {
                    connection.Dispose();
                }
            }
        }

        public int GetDatabaseConnectionPoolSize(string databaseName) {
            if (String.IsNullOrEmpty(databaseName)) throw new ArgumentNullException("databaseName");
            var connectionPool = _connectionPools
                .SingleOrDefault(pool => pool.DatabaseConnectionInfo.DatabaseName == databaseName);

            if (connectionPool == null)
                throw new ArgumentException("Database name does not have a connection pool.", databaseName);

            return connectionPool.CurrentSize;
        }

        public void Dispose() {
            DropConnectionPools();
        }

        private void DropConnectionPools() {
            lock (_syncRoot) {
                foreach (var pool in _connectionPools) {
                    pool.DropConnections();
                }
                _connectionPools.Clear();
            }
        }
    }
}
