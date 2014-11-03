namespace Orient.Client.API
{
    public interface IServerCredentials
    {
        string RootUsername { get; }
        string RootPassword { get; }
    }
}
