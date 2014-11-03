using System;
using System.Globalization;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Orient.Client.API;
using Orient.Client.API.Exceptions;
using Orient.Client.API.Types;

namespace Orient.Client.Protocol.Serializers
{
    internal static class RecordSerializer
    {
        internal static string Serialize(ODocument document) {
            if (!document.HasField("@OClassName")) {
                throw new OException(OExceptionType.Serialization, "Document doesn't contain @OClassName field which is required for serialization.");
            }

            return document.GetField<string>("@OClassName") + "@" + SerializeDocument(document);
        }

        #region Deserialize

        internal static ODocument Deserialize(ORID orid, int version, ORecordType type, short classId, byte[] rawRecord) {
            var document = new ODocument {ORID = orid, OVersion = version, OType = type, OClassId = classId};

            var recordString = BinarySerializer.ToString(rawRecord).Trim();

            return Deserialize(recordString, document);
        }

        public static ODocument Deserialize(string recordString, ODocument document) {
            var atIndex = recordString.IndexOf('@');
            var colonIndex = recordString.IndexOf(':');
            var index = 0;

            // parse class name
            if ((atIndex != -1) && (atIndex < colonIndex)) {
                document.OClassName = recordString.Substring(0, atIndex);
                index = atIndex + 1;
            }

            // start document parsing with first field name
            do {
                index = ParseFieldName(index, recordString, document);
            } while (index < recordString.Length);

            return document;
        }

        internal static ODocument Deserialize(string recordString) {
            return Deserialize(recordString, new ODocument());

        }

        #endregion

        #region Serialization private methods

        private static string SerializeDocument(ODocument document) {
            var bld = new StringBuilder();

            if (document.Keys.Count <= 0) return bld.ToString();
            foreach (var field in document) {
                // serialize only fields which doesn't start with @ character
                if ((field.Key.Length <= 0) || (field.Key[0] == '@')) continue;
                if (bld.Length > 0)
                    bld.Append(",");


                bld.AppendFormat("{0}:{1}", field.Key, SerializeValue(field.Value));
            }


            return bld.ToString();
        }

        private static string SerializeValue(object value) {
            if (value == null)
                return string.Empty;

            var valueType = value.GetType();

            switch (Type.GetTypeCode(valueType)) {
                case TypeCode.Empty:
                    // null case is empty
                    break;
                case TypeCode.Boolean:
                    return value.ToString().ToLower();
                case TypeCode.Byte:
                    return value + "b";
                case TypeCode.Int16:
                    return value + "s";
                case TypeCode.Int32:
                    return value.ToString();
                case TypeCode.Int64:
                    return value + "l";
                case TypeCode.Single:
                    return ((float)value).ToString(CultureInfo.InvariantCulture) + "f";
                case TypeCode.Double:
                    return ((double)value).ToString(CultureInfo.InvariantCulture) + "d";
                case TypeCode.Decimal:
                    return ((decimal)value).ToString(CultureInfo.InvariantCulture) + "c";
                case TypeCode.DateTime:
                    var unixEpoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    return ((long)((DateTime)value - unixEpoch).TotalMilliseconds) + "t";
                case TypeCode.String:
                case TypeCode.Char:
                    // strings must escape these characters:
                    // " -> \"
                    // \ -> \\
                    var stringValue = value.ToString();
                    // escape quotes
                    stringValue = stringValue.Replace("\\", "\\\\");
                    // escape backslashes
                    stringValue = stringValue.Replace("\"", "\\" + "\"");

                    return "\"" + stringValue + "\"";
                case TypeCode.Object:
                    return SerializeObjectValue(value, valueType);
            }

            throw new NotImplementedException();
        }

        private static string SerializeObjectValue(object value, Type valueType) {
            var bld = new StringBuilder();

            if ((valueType.IsArray) || (valueType.IsGenericType)) {
                bld.Append(valueType.Name == "HashSet`1" ? "<" : "[");

                var collection = (IEnumerable)value;

                var first = true;
                foreach (var val in collection) {
                    if (!first)
                        bld.Append(",");

                    first = false;
                    bld.Append(SerializeValue(val));
                }

                bld.Append(valueType.Name == "HashSet`1" ? ">" : "]");
            }
                // if property is ORID type it needs to be serialized as ORID
            else if (valueType.IsClass && (valueType.Name == "ORID")) {
                bld.Append(((ORID)value).RID);
            } else if (valueType.IsClass && (valueType.Name == "ODocument")) {
                bld.AppendFormat("({0})", SerializeDocument((ODocument)value));
            }
            return bld.ToString();
        }

        #endregion

        #region Deserialization private methods

        private static int ParseFieldName(int i, string recordString, ODocument document) {
            var startIndex = i;

            var iColonPos = recordString.IndexOf(':', i);
            if (iColonPos == -1)
                return recordString.Length;

            i = iColonPos;

            // parse field name string from raw document string
            var fieldName = recordString.Substring(startIndex, i - startIndex);
            var pos = fieldName.IndexOf('@');
            if (pos > 0) {
                fieldName = fieldName.Substring(pos + 1, fieldName.Length - pos - 1);
            }

            fieldName = fieldName.Replace("\"", "");

            document.Add(fieldName, null);

            // move to position after colon (:)
            i++;

            // check if it's not the end of document which means that current field has null value
            if (i == recordString.Length) {
                return i;
            }

            // check what follows after parsed field name and start parsing underlying type
            switch (recordString[i]) {
                case '"':
                    i = ParseString(i, recordString, document, fieldName);
                    break;
                case '#':
                    i = ParseRecordID(i, recordString, document, fieldName);
                    break;
                case '(':
                    i = ParseEmbeddedDocument(i, recordString, document, fieldName);
                    break;
                case '[':
                    i = ParseList(i, recordString, document, fieldName);
                    break;
                case '<':
                    i = ParseSet(i, recordString, document, fieldName);
                    break;
                case '{':
                    i = ParseMap(i, recordString, document, fieldName);
                    break;
                case '%':
                    i = ParseRidBags(i, recordString, document, fieldName);
                    break;
                default:
                    i = ParseValue(i, recordString, document, fieldName);
                    break;
            }

            // check if it's not the end of document which means that current field has null value
            if (i == recordString.Length) {
                return i;
            }

            // single string value was parsed and we need to push the index if next character is comma
            if (recordString[i] == ',') {
                i++;
            }

            return i;
        }

        private static int ParseString(int i, string recordString, ODocument document, string fieldName) {
            // move to the inside of string
            i++;

            var startIndex = i;

            // search for end of the parsed string value
            while (recordString[i] != '"') {
                // strings must escape these characters:
                // " -> \"
                // \ -> \\
                // therefore there needs to be a check for valid end of the string which
                // is quote character that is not preceeded by backslash character \
                if ((recordString[i] == '\\') && (recordString[i + 1] == '"')) {
                    i = i + 2;
                } else {
                    i++;
                }
            }

            var value = recordString.Substring(startIndex, i - startIndex);
            // escape quotes
            value = value.Replace("\\" + "\"", "\"");
            // escape backslashes
            value = value.Replace("\\\\", "\\");

            // assign field value
            if (document[fieldName] == null) {
                document[fieldName] = value;
            } else if (document[fieldName] is HashSet<object>) {
                ((HashSet<object>)document[fieldName]).Add(value);
            } else {
                ((List<object>)document[fieldName]).Add(value);
            }

            // move past the closing quote character
            i++;

            return i;
        }

        private static int ParseRecordID(int i, string recordString, ODocument document, string fieldName) {
            var startIndex = i;

            // search for end of parsed record ID value
            while (
                (i < recordString.Length) &&
                (recordString[i] != ',') &&
                (recordString[i] != ')') &&
                (recordString[i] != ']') &&
                (recordString[i] != '>')) {
                i++;
            }


            //assign field value
            if (document[fieldName] == null) {
                // there is a special case when OEdge InV/OutV fields contains only single ORID instead of HashSet<ORID>
                // therefore single ORID should be deserialized into HashSet<ORID> type
                if (fieldName.Equals("in_") || fieldName.Equals("out_")) {
                    document[fieldName] = new HashSet<ORID>();
                    ((HashSet<ORID>)document[fieldName]).Add(new ORID(recordString, startIndex));
                } else {
                    document[fieldName] = new ORID(recordString, startIndex);
                }
            } else if (document[fieldName] is HashSet<object>) {
                ((HashSet<object>)document[fieldName]).Add(new ORID(recordString, startIndex));
            } else {
                ((List<object>)document[fieldName]).Add(new ORID(recordString, startIndex));
            }

            return i;
        }

        private static int ParseMap(int i, string recordString, ODocument document, string fieldName) {
            var startIndex = i;
            var nestingLevel = 1;

            // search for end of parsed map
            while ((i < recordString.Length) && (nestingLevel != 0)) {
                // check for beginning of the string to prevent finding an end of map within string value
                if (recordString[i + 1] == '"') {
                    // move to the beginning of the string
                    i++;

                    // go to the end of string
                    while ((i < recordString.Length) && (recordString[i + 1] != '"')) {
                        i++;
                    }

                    // move to the end of string
                    i++;
                } else if (recordString[i + 1] == '{') {
                    // move to the beginning of the string
                    i++;

                    nestingLevel++;
                } else if (recordString[i + 1] == '}') {
                    // move to the beginning of the string
                    i++;

                    nestingLevel--;
                } else {
                    i++;
                }
            }

            // move past the closing bracket character
            i++;

            // do not include { and } in field value
            startIndex++;

            //assign field value
            if (document[fieldName] == null) {
                document[fieldName] = recordString.Substring(startIndex, i - 1 - startIndex);
            } else if (document[fieldName] is HashSet<object>) {
                ((HashSet<object>)document[fieldName]).Add(recordString.Substring(startIndex, i - 1 - startIndex));
            } else {
                ((List<object>)document[fieldName]).Add(recordString.Substring(startIndex, i - 1 - startIndex));
            }

            return i;
        }

        private static int ParseValue(int i, string recordString, ODocument document, string fieldName) {
            var startIndex = i;

            // search for end of parsed field value
            while (
                (i < recordString.Length) &&
                (recordString[i] != ',') &&
                (recordString[i] != ')') &&
                (recordString[i] != ']') &&
                (recordString[i] != '>')) {
                i++;
            }

            // determine the type of field value

            var stringValue = recordString.Substring(startIndex, i - startIndex);
            var value = new object();

            if (stringValue.Length > 0) {
                // binary content
                if ((stringValue.Length > 2) && (stringValue[0] == '_') && (stringValue[stringValue.Length - 1] == '_')) {
                    stringValue = stringValue.Substring(1, stringValue.Length - 2);

                    // need to be able for base64 encoding which requires content to be devidable by 4
                    var mod4 = stringValue.Length % 4;

                    if (mod4 > 0) {
                        stringValue += new string('=', 4 - mod4);
                    }

                    value = Convert.FromBase64String(stringValue);
                }
                    // datetime or date
                else if ((stringValue.Length > 2) && (stringValue[stringValue.Length - 1] == 't') || (stringValue[stringValue.Length - 1] == 'a')) {
                    // Unix timestamp is miliseconds past epoch
                    var epoch = new DateTime(1970, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
                    var foo = stringValue.Substring(0, stringValue.Length - 1);
                    var d = double.Parse(foo);
                    value = epoch.AddMilliseconds(d).ToUniversalTime();
                }
                    // boolean
                else if ((stringValue.Length > 2) && (stringValue == "true") || (stringValue == "false")) {
                    value = (stringValue == "true");
                }
                    // numbers
                else {
                    var lastCharacter = stringValue[stringValue.Length - 1];

                    switch (lastCharacter) {
                        case 'b':
                            value = byte.Parse(stringValue.Substring(0, stringValue.Length - 1));
                            break;
                        case 's':
                            value = short.Parse(stringValue.Substring(0, stringValue.Length - 1));
                            break;
                        case 'l':
                            value = long.Parse(stringValue.Substring(0, stringValue.Length - 1));
                            break;
                        case 'f':
                            value = float.Parse(stringValue.Substring(0, stringValue.Length - 1), CultureInfo.InvariantCulture);
                            break;
                        case 'd':
                            value = double.Parse(stringValue.Substring(0, stringValue.Length - 1), CultureInfo.InvariantCulture);
                            break;
                        case 'c':
                            value = decimal.Parse(stringValue.Substring(0, stringValue.Length - 1), CultureInfo.InvariantCulture);
                            break;
                        default:
                            value = int.Parse(stringValue);
                            break;
                    }
                }
            }
                // null
            else if (stringValue.Length == 0) {
                value = null;
            }

            //assign field value
            if (document[fieldName] == null) {
                document[fieldName] = value;
            } else if (document[fieldName] is HashSet<object>) {
                ((HashSet<object>)document[fieldName]).Add(value);
            } else {
                ((List<object>)document[fieldName]).Add(value);
            }

            return i;
        }

        /// <summary>
        /// Parse RidBags ex. %[content:binary]; where [content:binary] is the actual binary base64 content.
        /// </summary>
        /// <param name="i"></param>
        /// <param name="recordString"></param>
        /// <param name="document"></param>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        private static int ParseRidBags(int i, string recordString, ODocument document, string fieldName) {
            //move to first base64 char
            i++;

            var builder = new StringBuilder();

            while (recordString[i] != ';') {
                builder.Append(recordString[i]);
                i++;
            }
            
 
            var value = Convert.FromBase64String(builder.ToString());

            // use a list as it preserves order at this stage which may be important when using ordered edges
            var orids = new List<ORID>();

            using(var stream = new MemoryStream(value))
            using (var reader = new BinaryReader(stream))
            {
                var flags = reader.ReadByte(); // 1 - embedded, 0 - tree-based 

                if ((flags & 1) == 0)
                    throw new NotSupportedException("No support for tree based ridBags yet ");

                if ((flags & 1) == 1) { 
                    var entriesCount = reader.ReadInt32EndianAware();
                    for (var j = 0; j < entriesCount; j++) {
                        var clusterid = reader.ReadInt16EndianAware();
                        var clusterposition = reader.ReadInt64EndianAware();
                        orids.Add(new ORID(clusterid, clusterposition));
                    }
                }

                if ((flags & 2) == 2){
                    throw new NotSupportedException("No support for uuid parsing yet");
                }
            
            }
            
            document[fieldName] = orids;

            //move past ';'
            i++;

            return i;
        }

        private static int ParseEmbeddedDocument(int i, string recordString, ODocument document, string fieldName) {
            // move to the inside of embedded document (go past starting bracket character)
            i++;


            if ((i < 15) && (recordString.Length > 15) && (recordString.Substring(i, 15).Equals("ORIDs@pageSize:"))) {
                var linkCollection = new OLinkCollection();
                i = ParseLinkCollection(i, recordString, linkCollection);
                document[fieldName] = linkCollection;
            } else {
                // create new dictionary which would hold K/V pairs of embedded document
                var embeddedDocument = new ODocument();

                // assign embedded object
                if (document[fieldName] == null) {
                    document[fieldName] = embeddedDocument;
                } else if (document[fieldName] is HashSet<object>) {
                    ((HashSet<object>)document[fieldName]).Add(embeddedDocument);
                } else {
                    ((List<object>)document[fieldName]).Add(embeddedDocument);
                }

                // start parsing field names until the closing bracket of embedded document is reached
                while (recordString[i] != ')') {
                    i = ParseFieldName(i, recordString, embeddedDocument);
                }
            }

            // move past close bracket of embedded document
            i++;

            return i;
        }

        private static int ParseLinkCollection(int i, string recordString, OLinkCollection linkCollection) {
            // move to the start of pageSize value
            i += 15;

            var index = recordString.IndexOf(',', i);

            linkCollection.PageSize = int.Parse(recordString.Substring(i, index - i));

            // move to root value
            i = index + 6;
            index = recordString.IndexOf(',', i);

            linkCollection.Root = new ORID(recordString.Substring(i, index - i));

            // move to keySize value
            i = index + 9;
            index = recordString.IndexOf(')', i);

            linkCollection.KeySize = int.Parse(recordString.Substring(i, index - i));

            // move past close bracket of link collection
            i++;

            return i;
        }

        private static int ParseList(int i, string recordString, ODocument document, string fieldName) {
            // move to the first element of this list
            i++;

            if (document[fieldName] == null) {
                document[fieldName] = new List<object>();
            }

            while (recordString[i] != ']') {
                // check what follows after parsed field name and start parsing underlying type
                switch (recordString[i]) {
                    case '"':
                        i = ParseString(i, recordString, document, fieldName);
                        break;
                    case '#':
                        i = ParseRecordID(i, recordString, document, fieldName);
                        break;
                    case '(':
                        i = ParseEmbeddedDocument(i, recordString, document, fieldName);
                        break;
                    case '{':
                        i = ParseMap(i, recordString, document, fieldName);
                        break;
                    case ',':
                        i++;
                        break;
                    default:
                        i = ParseValue(i, recordString, document, fieldName);
                        break;
                }
            }

            // move past closing bracket of this list
            i++;

            return i;
        }

        private static int ParseSet(int i, string recordString, ODocument document, string fieldName) {
            // move to the first element of this set
            i++;

            if (document[fieldName] == null) {
                document[fieldName] = new HashSet<object>();
            }

            while (recordString[i] != '>') {
                // check what follows after parsed field name and start parsing underlying type
                switch (recordString[i]) {
                    case '"':
                        i = ParseString(i, recordString, document, fieldName);
                        break;
                    case '#':
                        i = ParseRecordID(i, recordString, document, fieldName);
                        break;
                    case '(':
                        i = ParseEmbeddedDocument(i, recordString, document, fieldName);
                        break;
                    case '{':
                        i = ParseMap(i, recordString, document, fieldName);
                        break;
                    case ',':
                        i++;
                        break;
                    default:
                        i = ParseValue(i, recordString, document, fieldName);
                        break;
                }
            }

            // move past closing bracket of this set
            i++;

            return i;
        }

        #endregion
    }
}
