using System.Collections.Generic;
using System.Linq;
using Orient.Client.API.Types;
using Orient.Client.Protocol;

namespace Orient.Client.API.Query
{
    public class OCommandResult
    {
        private readonly ODocument _document;

        internal OCommandResult(ODocument document)
        {
            _document = document;
        }

        public ODocument ToSingle()
        {
            ODocument document = null;

            switch (_document.GetField<PayloadStatus>("PayloadStatus"))
            {
                case PayloadStatus.SingleRecord:
                    document = _document.GetField<ODocument>("Content");
                    break;
                case PayloadStatus.RecordCollection:
                    document = _document.GetField<List<ODocument>>("Content").FirstOrDefault();
                    break;
            }

            return document;
        }

        public List<ODocument> ToList()
        {
            return _document.GetField<List<ODocument>>("Content");
        }

        public ODocument ToDocument()
        {
            return _document;
        }
    }
}
