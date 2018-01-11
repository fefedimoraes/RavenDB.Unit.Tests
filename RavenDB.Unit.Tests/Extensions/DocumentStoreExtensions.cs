using System;
using System.IO;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Server.Responders;

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

        public static DocumentStore InitializeWithoutCreatingDatabase(this DocumentStore documentStore)
        {
            var databaseName = documentStore.DefaultDatabase;
            documentStore.DefaultDatabase = null;
            documentStore.Initialize();
            documentStore.DefaultDatabase = databaseName;
            return documentStore;
        }

        public static IDocumentStore CreateDatabase(this IDocumentStore documentStore, string databaseName)
        {
            var newDatabaseDocument = new DatabaseDocument
            {
                Id = databaseName,
                Settings =
                    {
                        { "Raven/ActiveBundles", "Replication" },
                        { "Raven/DataDir", Path.Combine("~/databases/", databaseName) },
                        { "Raven/PluginsDirectory", "RavenPlugins" }
                    }
            };
            return documentStore.WithDatabase(newDatabaseDocument);
        }

        public static IDocumentStore WithDatabase(this IDocumentStore documentStore, DatabaseDocument databaseDocument)
        {
            documentStore.DatabaseCommands.CreateDatabase(databaseDocument);
            return documentStore;
        }
    }
}