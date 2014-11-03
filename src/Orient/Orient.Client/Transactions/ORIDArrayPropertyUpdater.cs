using System.Collections.Generic;
using System.Reflection;
using Orient.Client.API.Types;

namespace Orient.Client.Transactions
{
    internal class ORIDArrayPropertyUpdater<TTarget> : ORIDPropertyUpdater<TTarget, ORID[]>
    {
        public ORIDArrayPropertyUpdater(PropertyInfo propertyInfo) : base(propertyInfo)
        {
        }

        public override void Update(object oTarget, Dictionary<ORID, ORID> mappings)
        {
            var orids = GetValue(oTarget);
            if (orids == null)
                return;
            for (var i = 0; i < orids.Length; i++)
            {
                ORID replacement;
                if (mappings.TryGetValue(orids[i], out replacement))
                {
                    orids[i] = replacement;
                }
            }
        }
    }
}