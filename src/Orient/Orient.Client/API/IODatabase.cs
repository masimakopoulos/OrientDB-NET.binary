using System;
using System.Collections.Generic;
using Orient.Client.API.Query;
using Orient.Client.API.Types;

namespace Orient.Client.API
{
    public interface IODatabase
    {
        string Name { get; }
        IDictionary<ORID, ODocument> ClientCache { get; }
        OSqlCreate Create { get; }
        OSqlDelete Delete { get; }
        OLoadRecord Load { get; }
        ORecordMetadata Metadata { get; }
        OSqlSchema Schema { get; }
        OTransaction Transaction { get; }
        Guid Id { get; }
        IEnumerable<OCluster> GetClusters();
        short GetClusterIdFor(string className);
        //void AddCluster(string className, short clusterId);
        OSqlSelect Select(params string[] projections);
        OSqlInsert Insert();
        OSqlInsert Insert<T>(T obj);
        OSqlUpdate Update();
        OSqlUpdate Update(ORID orid);
        OSqlUpdate Update<T>(T obj);
        List<ODocument> Query(string sql);
        List<ODocument> Query(string sql, string fetchPlan);
        List<ODocument> Gremlin(string query);
        List<ODocument> JavaScript(string query);
        OCommandResult Command(string sql);
        void SaveChanges();
    }
}