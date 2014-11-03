using System.Collections.Generic;

namespace Orient.Client.API.Types
{
    public class OVertex : ODocument
    {
        public OVertex() {
            OClassName = "V";
        }

        [OProperty(Alias = "in_", Serializable = false)]
        public HashSet<ORID> InE {
            get { return GetField<HashSet<ORID>>("in_"); }
        }

        [OProperty(Alias = "out_", Serializable = false)]
        public HashSet<ORID> OutE {
            get { return GetField<HashSet<ORID>>("out_"); }
        }
    }
}
