using System;
using System.Reflection;
using Orient.Client.API.Types;

namespace Orient.Client.Mapping
{
    interface IFieldMapping
    {
        void MapToObject(ODocument document, object typedObject);
        void MapToDocument(object typedObject, ODocument document);
    }

    internal abstract class FieldMapping<TTarget> : IFieldMapping
    {
        protected readonly PropertyInfo PropertyInfo;
        protected readonly string FieldPath;
        private Action<TTarget, object> _setter;
        private Func<TTarget, object> _getter;

        protected FieldMapping(PropertyInfo propertyInfo, string fieldPath) {
            if (propertyInfo != null) {
                _setter = FastPropertyAccessor.BuildUntypedSetter<TTarget>(propertyInfo);
                _getter = FastPropertyAccessor.BuildUntypedGetter<TTarget>(propertyInfo);
            }
            PropertyInfo = propertyInfo;
            FieldPath = fieldPath;
        }

        protected object GetPropertyValue(TTarget target) {
            return _getter(target);
        }

        protected void SetPropertyValue(TTarget target, object value) {
            _setter(target, value);
        }


        protected abstract void MapToObject(ODocument document, TTarget typedObject);
        protected abstract void MapToDocument(TTarget typedObject, ODocument document);

        public void MapToObject(ODocument document, object typedObject) {
            MapToObject(document, (TTarget)typedObject);
        }

        public void MapToDocument(object typedObject, ODocument document) {
            MapToDocument((TTarget)typedObject, document);
        }
    }

    internal abstract class NamedFieldMapping<TTarget> : FieldMapping<TTarget>
    {
        protected NamedFieldMapping(PropertyInfo propertyInfo, string fieldPath)
            : base(propertyInfo, fieldPath) {
        }

        protected override void MapToObject(ODocument document, TTarget typedObject) {
            if (document.HasField(FieldPath))
                MapToNamedField(document, typedObject);
        }


        protected abstract void MapToNamedField(ODocument document, TTarget typedObject);
    }
}