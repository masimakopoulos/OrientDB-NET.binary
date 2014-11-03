namespace Orient.Client.API.Types
{
    public abstract class GenericEdge<TFrom, TTo> : OBaseRecord
    {
        protected GenericEdge() {
            OClassName = GetType().Name;
        }

        [OProperty(Alias = "in", Serializable = false)]
        public ORID In { get; set; }

        [OProperty(Alias = "out", Serializable = false)]
        public ORID Out { get; set; }

        public bool IsLightWeightEdge() {
            return ORID.ClusterId == -1 && ORID.ClusterPosition == -1;
       }
    }
}
