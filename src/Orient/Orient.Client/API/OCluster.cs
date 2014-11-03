
using System;

namespace Orient.Client.API
{
    public class OCluster : IEquatable<OCluster>
    {
        public short Id { get; set; }
        public string Name { get; set; }
        public OClusterType Type { get; set; }

        [Obsolete]
        public string Location { get; set; }

        [Obsolete]
        public short DataSegmentID { get; set; }

        [Obsolete]
        public string DataSegmentName { get; set; }

        public override bool Equals(object obj) {
            if (obj == null) return false;

            // if parameter cannot be cast to ORID return false.
            var other = obj as OCluster;

            return other != null && Equals(other);
        }

        public override int GetHashCode() {
            return (Id*17) ^ Name.GetHashCode() ^ Type.GetHashCode();
        }

        public static bool operator ==(OCluster left, OCluster right) {
            if (ReferenceEquals(left, right)) {
                return true;
            }

            if (((object) left == null) || ((object) right == null)) {
                return false;
            }

            return left.Equals(right);
        }

        public static bool operator !=(OCluster left, OCluster right) {
            return !(left == right);
        }

        public bool Equals(OCluster other) {
            if (other == null)
                return false;

            return Id == other.Id && String.Equals(Name, other.Name, StringComparison.InvariantCultureIgnoreCase) &&
                   Type == other.Type;
        }
    }
}