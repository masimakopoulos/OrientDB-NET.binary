using System;

namespace Orient.Client.API.Types
{
    public class OEdge : ODocument
    {
        public OEdge() {
            OClassName = "E";
        }

        [OProperty(Alias = "in", Serializable = false)]
        public ORID InV {
            get { return GetField<ORID>("in"); }
        }

        [OProperty(Alias = "out", Serializable = false)]
        public ORID OutV {
            get { return GetField<ORID>("out"); }
        }

        [OProperty(Alias = "label", Serializable = false)]
        public string Label {
            get {
                var label = GetField<string>("@OClassName");
                return String.IsNullOrEmpty(label) ? GetType().Name : label;
            }
        }
    }
}