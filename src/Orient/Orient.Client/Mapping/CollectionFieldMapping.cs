using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Orient.Client.API.Types;

namespace Orient.Client.Mapping
{
    internal class ListNamedFieldMapping<TTarget> : CollectionNamedFieldMapping<TTarget>
    {
        private readonly Func<object> _listFactory;

        public ListNamedFieldMapping(PropertyInfo propertyInfo, string fieldPath) : base(propertyInfo, fieldPath) {
            _listFactory = FastConstructor.BuildConstructor(PropertyInfo.PropertyType);
        }

        protected override object CreateCollectionInstance(int collectionSize) {
            return _listFactory();
        }

        protected override void AddItemToCollection(object collection, int index, object item)  {
            ((IList)collection).Add(item);
        }
    }

    internal class ArrayNamedFieldMapping<TTarget> : CollectionNamedFieldMapping<TTarget>
    {
        private readonly Func<int, object> _arrayFactory;

        public ArrayNamedFieldMapping(PropertyInfo propertyInfo, string fieldPath)
            : base(propertyInfo, fieldPath) {
            _arrayFactory = FastConstructor.BuildConstructor<int>(propertyInfo.PropertyType);
        }


        protected override object CreateCollectionInstance(int collectionSize) {
            return _arrayFactory(collectionSize);
        }

        protected override void AddItemToCollection(object collection, int index, object item) {
            ((IList)collection)[index] = item;
        }
    }
}