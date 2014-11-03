namespace Orient.Client.API
{
    public interface IServerAddress
    {
        string Hostname { get; }
        int Port { get; }
    }
}
