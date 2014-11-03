﻿using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Orient.Client.API.Types;

namespace Orient.Client.Mapping
{
    internal abstract class CollectionNamedFieldMapping<TTarget> : NamedFieldMapping<TTarget>
    {
        private readonly TypeMapperBase _mapper;
        private readonly Type _targetElementType;
        private readonly bool _needsMapping;
        private readonly Func<object> _elementFactory;

        protected CollectionNamedFieldMapping(PropertyInfo propertyInfo, string fieldPath)
            : base(propertyInfo, fieldPath) {
            _targetElementType = GetTargetElementType();
            _needsMapping = !NeedsNoConversion(_targetElementType);
            if (_needsMapping) {
                _mapper = TypeMapperBase.GetInstanceFor(_targetElementType);
                _elementFactory = FastConstructor.BuildConstructor(_targetElementType);
            }
        }

        protected abstract object CreateCollectionInstance(int collectionSize);
        protected abstract void AddItemToCollection(object collection, int index, object item);

        protected override void MapToNamedField(ODocument document, TTarget typedObject) {
            object sourcePropertyValue = document.GetField<object>(FieldPath);

            IList collection = sourcePropertyValue as IList;
            if (collection == null)
                // if we only have one item currently stored (but scope for more) we create a temporary list and put our single item in it.
            {
                collection = new ArrayList();
                if (sourcePropertyValue != null)
                    collection.Add(sourcePropertyValue);
            }

            // create instance of property type
            var collectionInstance = CreateCollectionInstance(collection.Count);

            for (int i = 0; i < collection.Count; i++) {
                var t = collection[i];
                object oMapped = t;
                if (_needsMapping) {
                    object element = _elementFactory();
                    _mapper.ToObject((ODocument) t, element);
                    oMapped = element;
                }

                AddItemToCollection(collectionInstance, i, oMapped);
            }

            SetPropertyValue(typedObject, collectionInstance);
        }

        private Type GetTargetElementType() {
            if (PropertyInfo.PropertyType.IsArray)
                return PropertyInfo.PropertyType.GetElementType();
            if (PropertyInfo.PropertyType.IsGenericType)
                return PropertyInfo.PropertyType.GetGenericArguments().First();

            throw new NotImplementedException();

        }

        private static bool NeedsNoConversion(Type elementType) {
            return elementType.IsPrimitive ||
                   (elementType == typeof (string)) ||
                   (elementType == typeof (DateTime)) ||
                   (elementType == typeof (decimal)) ||
                   (elementType == typeof (ORID));
        }

        protected override void MapToDocument(TTarget typedObject, ODocument document) {
            var targetElementType = _needsMapping ? typeof (ODocument) : _targetElementType;
            var listType = typeof (List<>).MakeGenericType(targetElementType);
            var targetList = (IList) Activator.CreateInstance(listType);

            var sourceList = (IEnumerable) GetPropertyValue(typedObject);
            if (sourceList != null) {
                foreach (var item in sourceList)
                    targetList.Add(_needsMapping ? _mapper.ToDocument(item) : item);
            }

            document.SetField(FieldPath, targetList);
        }
    }
}