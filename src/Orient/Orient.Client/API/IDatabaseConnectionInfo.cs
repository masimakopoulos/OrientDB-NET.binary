namespace Orient.Client.API
{
    public interface IDatabaseConnectionInfo
    {
        string DatabaseName { get; }
        ODatabaseType DatabaseType { get; }
        string Username { get; }
        string Password { get; }
        int PoolSize { get; }
    }
}
