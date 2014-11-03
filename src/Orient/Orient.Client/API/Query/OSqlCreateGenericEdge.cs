using Orient.Client.API.Types;
using Orient.Client.Protocol;

namespace Orient.Client.API.Query
{
    public interface IOSqlCreateGenericEdge<TFrom, TTo>
    {
        IOSqlCreateGenericEdge<TFrom, TTo> Edge(GenericEdge<TFrom, TTo> obj);
        IOSqlCreateGenericEdge<TFrom, TTo> From(TFrom fromVertex);
        IOSqlCreateGenericEdge<TFrom, TTo> To(TTo toVertex);
        OEdge Run();
        T Run<T>() where T : class, new();
    }

    public class OSqlCreateGenericEdge<TFrom, TTo> : IOSqlCreateGenericEdge<TFrom, TTo>
    {
        private readonly ΟSqlCreateEdgeViaSql _sqlCreateEdgeViaSql;
        public OSqlCreateGenericEdge(Connection connection){
            _sqlCreateEdgeViaSql = new ΟSqlCreateEdgeViaSql(connection);
        }

        public IOSqlCreateGenericEdge<TFrom, TTo> Edge(GenericEdge<TFrom, TTo> obj) {
            _sqlCreateEdgeViaSql.Edge(obj);
            return this;
        }

        public IOSqlCreateGenericEdge<TFrom, TTo> From(TFrom fromVertex) {
            _sqlCreateEdgeViaSql.From(fromVertex);
            return this;
        }

        public IOSqlCreateGenericEdge<TFrom, TTo> To(TTo toVertex) {
            _sqlCreateEdgeViaSql.To(toVertex);
            return this;
        }

        public OEdge Run() {
            return _sqlCreateEdgeViaSql.Run();
        }

        public T Run<T>() where T : class, new() {
            return _sqlCreateEdgeViaSql.Run<T>();
        }

        public override string ToString() {
            return _sqlCreateEdgeViaSql.ToString();
        }
    }
}
