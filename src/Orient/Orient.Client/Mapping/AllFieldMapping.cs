using System;
using Orient.Client.API.Types;

namespace Orient.Client.Mapping
{
    internal class AllFieldMapping<TTarget> : FieldMapping<TTarget>
    {
        public AllFieldMapping()
            : base(null, null) {
        }

        protected override void MapToObject(ODocument document, TTarget typedObject) {
            var target = (ODocument)(object)typedObject;
            foreach (var item in document) {
                target.SetField(item.Key, item.Value);
            }
        }

        protected override void MapToDocument(TTarget typedObject, ODocument document) {
            if (document == null) throw new ArgumentNullException("document");

            var source = (ODocument)(object)typedObject;
            if (source == null) return;
            foreach (var item in source) {
                document.SetField(item.Key, item.Value);
            }
        }
    }
}