using System;
using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Operations
{
    internal enum RecordType
    {
        Update = 1,
        Delete = 2,
        Create = 3,
    }

    class TransactionRecord
    {
        public TransactionRecord(RecordType recordType, ODocument document = null)
        {
            RecordType = recordType;
            Document = document;
        }

        public ODocument Document { get; set; }
        public RecordType RecordType { get; private set; }
        public IBaseRecord Object { get; set; }

        public ORID ORID
        {
            get
            {
                if (Document != null)
                    return Document.ORID;

                return Object.ORID;
            }
            set
            {
                if (Document != null)
                    Document.ORID = value;
                if (Object != null)
                    Object.ORID = value;
            }
        }

        public int Version
        {
            get {
                return Document != null ? Document.OVersion : Object.OVersion;
            }
            set
            {
                if (Document != null)
                    Document.OVersion = value;
                if (Object != null)
                    Object.OVersion = value;
            }
        }

        public string OClassName
        {
            get {
                return Document != null ? Document.OClassName : Object.OClassName;
            }
        }


        public void AddToRequest(Request request) {
            var document = GetDocument();
            request.AddDataItem((byte)1); // undocumented but the java code does this
            request.AddDataItem((byte)RecordType);
            request.AddDataItem(ORID.ClusterId);
            request.AddDataItem(ORID.ClusterPosition);
            request.AddDataItem((byte)ORecordType.Document);

            switch (RecordType)
            {
                case RecordType.Create:
                    request.AddDataItem(document.Serialize());
                    break;
                case RecordType.Delete:
                    request.AddDataItem(Version);
                    break;
                case RecordType.Update:
                    request.AddDataItem(Version);
                    request.AddDataItem(document.Serialize());
                    if (ServerInfo.ProtocolVersion >= 23)
                        request.AddDataItem(document.IsContentChanged);
                    break;

                default:
                    throw new InvalidOperationException();
            }

        }

     

        private ODocument GetDocument()
        {
            return Document ?? (Document = ODocument.ToDocument(Object));
        }
    }
}