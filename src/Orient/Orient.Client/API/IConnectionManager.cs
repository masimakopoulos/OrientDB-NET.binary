using Orient.Client.Protocol;

namespace Orient.Client.API
{
    public interface IConnectionManager
    {
        Connection Get(IDatabaseConnectionInfo databaseConnectionInfo);
        void Release(Connection connection);
        int GetDatabaseConnectionPoolSize(string databaseName);
    }
}
