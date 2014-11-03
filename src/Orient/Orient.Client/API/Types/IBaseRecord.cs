namespace Orient.Client.API.Types
{
    public interface IBaseRecord
    {
        ORID ORID { get; set; }
        int OVersion { get; set; }
        short OClassId { get; set; }
        string OClassName { get; set; }
    }
}