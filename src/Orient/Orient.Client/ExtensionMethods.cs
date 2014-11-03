using System.Linq;
using System.Reflection;
using Orient.Client.API;

namespace Orient.Client
{
    internal static class ExtensionMethods
    {
        public static OProperty GetOPropertyAttribute(this PropertyInfo property) {
            return property.GetCustomAttributes(typeof (OProperty), true).OfType<OProperty>().FirstOrDefault();
        }
    }
}