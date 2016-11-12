using Raven.Abstractions.Replication;
using Raven.Client;

namespace RavenDB.Unit.Tests.Extensions
{
    public static class DocumentStoreExtensions
    {
        public static void EstablishReplicationTo(this IDocumentStore source, ReplicationDestination destination)
        {
            using (var session = source.OpenSession())
            {
                var replicationDocument = session.LoadOrStoreNew<ReplicationDocument>("Raven/Replication/Destinations");
                replicationDocument.Destinations.Add(destination);
                session.SaveChanges();
            }
        }
    }
}