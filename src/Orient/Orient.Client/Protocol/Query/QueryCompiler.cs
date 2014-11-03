using System.Collections.Generic;

namespace Orient.Client.Protocol.Query
{
    internal class QueryCompiler : Dictionary<string, string>
    {
        // add/append key values with prefix space
        internal void Append(string key, params string[] values)
        {
            if (ContainsKey(key))
            {
                this[key] += string.Join(" ", values);
            }
            else
            {
                Add(key, string.Join(" ", values));
            }
        }

        // add/overwrite key values
        internal void Unique(string key, params string[] values)
        {
            if (ContainsKey(key))
            {
                this[key] = string.Join(" ", values);
            }
            else
            {
                Add(key, string.Join(" ", values));
            }
        }

        internal bool HasKey(string key)
        {
            return ContainsKey(key);
        }

        // return key value if the specified key is present
        internal string Value(string key)
        {
            if (!ContainsKey(key))
            {
                return "";
            }

            return this[key];
        }

        // return key value from the given order of keyword where the last item has the highest priority
        internal string OrderedValue(params string[] order)
        {
            var value = "";

            foreach (var keyword in order)
            {
                var keyValue = Value(keyword);

                if (!string.IsNullOrEmpty(keyValue))
                {
                    value = keyValue;
                }
            }

            return value;
        }
    }
}
