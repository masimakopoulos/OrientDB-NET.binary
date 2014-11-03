using System.Reflection;
using Orient.Client.API;
using Orient.Client.API.Types;

namespace Orient.Client.Mapping
{
    internal class ORIDFieldMapping<TTarget> : FieldMapping<TTarget>
    {
        public ORIDFieldMapping(PropertyInfo propertyInfo) : base(propertyInfo, "ORID")
        {
            
        }

        protected override void MapToObject(ODocument document, TTarget typedObject)
        {
            SetPropertyValue(typedObject, document.ORID);
        }

        protected override void MapToDocument(TTarget typedObject, ODocument document)
        {
            document.ORID = (ORID)GetPropertyValue(typedObject);
        }
    }

    internal class OVersionFieldMapping<TTarget> : FieldMapping<TTarget>
    {
        public OVersionFieldMapping(PropertyInfo propertyInfo)
            : base(propertyInfo, "OVersion")
        {

        }

        protected override void MapToObject(ODocument document, TTarget typedObject)
        {
            SetPropertyValue(typedObject, document.OVersion);
        }

        protected override void MapToDocument(TTarget typedObject, ODocument document)
        {
            document.OVersion = (int)GetPropertyValue(typedObject);
        }
    }

    internal class OTypeFieldMapping<TTarget> : FieldMapping<TTarget>
    {
        public OTypeFieldMapping(PropertyInfo propertyInfo)
            : base(propertyInfo, "OType")
        {
            
        }

        protected override void MapToObject(ODocument document, TTarget typedObject)
        {
            SetPropertyValue(typedObject, document.OType);
        }

        protected override void MapToDocument(TTarget typedObject, ODocument document)
        {
            document.OType = (ORecordType)GetPropertyValue(typedObject);
        }
    }

    internal class OClassIdFieldMapping<TTarget> : FieldMapping<TTarget>
    {
        public OClassIdFieldMapping(PropertyInfo propertyInfo)
            : base(propertyInfo, "OClassId")
        {

        }

        protected override void MapToObject(ODocument document, TTarget typedObject)
        {
            SetPropertyValue(typedObject, document.OClassId);
        }

        protected override void MapToDocument(TTarget typedObject, ODocument document)
        {
            document.OClassId = (short)GetPropertyValue(typedObject);
        }
    }


    internal class OClassNameFieldMapping<TTarget> : FieldMapping<TTarget>
    {
        public OClassNameFieldMapping(PropertyInfo propertyInfo)
            : base(propertyInfo, "OClassName")
        {

        }

        protected override void MapToObject(ODocument document, TTarget typedObject)
        {
            SetPropertyValue(typedObject, document.OClassName);
        }

        protected override void MapToDocument(TTarget typedObject, ODocument document)
        {
            document.OClassName = (string) GetPropertyValue(typedObject);
        }
    }

}