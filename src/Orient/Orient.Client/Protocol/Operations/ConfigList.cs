using System.Collections.Generic;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal class ConfigList : IOperation
    {
        public Request Request(int sessionID)
        {
            var request = new Request();

            // standard request fields
            request.AddDataItem((byte)OperationType.CONFIG_LIST);
            request.AddDataItem(sessionID);
            return request;
        }

        public ODocument Response(Response response)
        {
            var document = new ODocument();

            if (response == null)
            {
                return document;
            }

            var reader = response.Reader;
            var len = reader.ReadInt16EndianAware();
            var configList = new Dictionary<string, string>();
            for (var i = 0; i < len; i++)
            {
                var key = reader.ReadInt32PrefixedString();
                var value = reader.ReadInt32PrefixedString();
                configList.Add(key, value);
            }
            document.SetField("config", configList);
            return document;
        }
    }
}
