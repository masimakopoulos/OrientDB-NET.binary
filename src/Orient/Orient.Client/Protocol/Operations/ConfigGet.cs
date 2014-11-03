using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class ConfigGet : IOperation
    {
        internal string ConfigKey { get; set; }

        public Request Request(int sessionID) {
            var request = new Request();

            // standard request fields
            request.AddDataItem((byte) OperationType.CONFIG_GET);
            request.AddDataItem(sessionID);

            request.AddDataItem(ConfigKey);
            return request;
        }

        public ODocument Response(Response response) {
            var document = new ODocument();

            if (response == null) {
                return document;
            }

            var reader = response.Reader;
            var value = reader.ReadInt32PrefixedString();
            document.SetField(ConfigKey, value);
            return document;
        }
    }
}
