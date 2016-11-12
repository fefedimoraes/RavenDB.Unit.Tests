using System;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using RavenDB.Unit.Tests.Extensions;
using Xunit;

namespace RavenDB.Unit.Tests
{
    public class TakeNewestConflictResolutionListenerUnitTests : IDisposable
    {
        public TakeNewestConflictResolutionListenerUnitTests()
        {
            const int masterServerPort = 9900;
            const int failoverServerPort = 9901;
            const string defaultDatabase = "ReplicatingDatabase";
            const string loopbackHttpAddress = "http://127.0.0.1";

            MasterServer = new EmbeddableDocumentStore
            {
                RunInMemory = true,
                UseEmbeddedHttpServer = true,
                Configuration =
                {
                    Port = masterServerPort,
                    Settings = { { "Raven/ActiveBundles", "Replication" } }
                }
            };

            FailoverServer = new EmbeddableDocumentStore
            {
                RunInMemory = true,
                UseEmbeddedHttpServer = true,
                Configuration =
                {
                    Port = failoverServerPort,
                    Settings = { { "Raven/ActiveBundles", "Replication" } }
                }
            };

            MasterClient = new DocumentStore
            {
                Url = $"{loopbackHttpAddress}:{masterServerPort}",
                DefaultDatabase = defaultDatabase,
                Conventions = new DocumentConvention
                {
                    DefaultQueryingConsistency = ConsistencyOptions.AlwaysWaitForNonStaleResultsAsOfLastWrite
                }
            };

            FailoverClient = new DocumentStore
            {
                Url = $"{loopbackHttpAddress}:{failoverServerPort}",
                DefaultDatabase = defaultDatabase,
                Conventions = new DocumentConvention
                {
                    DefaultQueryingConsistency = ConsistencyOptions.AlwaysWaitForNonStaleResultsAsOfLastWrite
                }
            };

            MasterClient.RegisterListener(new TakeNewestConflictResolutionListener());
            FailoverClient.RegisterListener(new TakeNewestConflictResolutionListener());

            MasterServer.Initialize();
            FailoverServer.Initialize();
            MasterClient.Initialize();
            FailoverClient.Initialize();

            MasterClient.ExecuteIndex(new RavenDocumentsByEntityName());
            FailoverClient.ExecuteIndex(new RavenDocumentsByEntityName());
        }

        public DocumentStore MasterClient { get; }

        public DocumentStore FailoverClient { get; }

        public EmbeddableDocumentStore MasterServer { get; }

        public EmbeddableDocumentStore FailoverServer { get; }

        public void Dispose()
        {
            MasterClient.Dispose();
            FailoverClient.Dispose();
            MasterServer.Dispose();
            FailoverServer.Dispose();
        }

        [Fact(DisplayName = "After establishing replication, conflicts should be solved when loading conflicting documents.")]
        public void UsingLoad()
        {
            const string userDocumentId = "users/someuser";

            // Arrange
            using (var masterSession = MasterClient.OpenSession())
            using (var failoverSession = FailoverClient.OpenSession())
            {
                masterSession.Store(new User { Name = "Name" }, userDocumentId);
                masterSession.SaveChanges();

                failoverSession.Store(new User { Name = "AnotherName" }, userDocumentId);
                failoverSession.SaveChanges();
            }

            MasterClient.EstablishReplicationTo(new ReplicationDestination
            {
                Url = FailoverClient.Url,
                Database = FailoverClient.DefaultDatabase,
                TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate
            });

            SpinWait.SpinUntil(() => FailoverClient.DatabaseCommands.HasConflict(userDocumentId));

            using (var session = FailoverClient.OpenSession())
            {
                // Act
                var user = session.Load<User>(userDocumentId);

                // Assert
                Assert.Equal(user.Name, "AnotherName");
            }
        }

        [Fact(DisplayName = "After establishing replication, conflicts should be solved when querying conflicting documents.")]
        public void UsingQuery()
        {
            const string userDocumentId = "users/someuser";

            // Arrange
            using (var masterSession = MasterClient.OpenSession())
            using (var failoverSession = FailoverClient.OpenSession())
            {
                masterSession.Store(new User { Name = "Name" }, userDocumentId);
                masterSession.SaveChanges();

                failoverSession.Store(new User { Name = "AnotherName" }, userDocumentId);
                failoverSession.SaveChanges();
            }

            MasterClient.EstablishReplicationTo(new ReplicationDestination
            {
                Url = FailoverClient.Url,
                Database = FailoverClient.DefaultDatabase,
                TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate
            });

            SpinWait.SpinUntil(() => FailoverClient.DatabaseCommands.HasConflict(userDocumentId));

            using (var session = FailoverClient.OpenSession())
            {
                // Act
                var users = session.Query<User>().ToList();

                // Assert
                Assert.Equal(1, users.Count);
                Assert.Equal(users[0].Name, "AnotherName");
            }
        }

        [Fact(DisplayName = "After establishing replication, conflicts should be solved when loading documents starting with prefix.")]
        public void UsingLoadStartingWith()
        {
            const string usersPrefix = "users/";
            const string userDocumentId = usersPrefix + "someuser";

            // Arrange
            using (var masterSession = MasterClient.OpenSession())
            using (var failoverSession = FailoverClient.OpenSession())
            {
                masterSession.Store(new User { Name = "Name" }, userDocumentId);
                masterSession.SaveChanges();

                failoverSession.Store(new User { Name = "AnotherName" }, userDocumentId);
                failoverSession.SaveChanges();
            }

            MasterClient.EstablishReplicationTo(new ReplicationDestination
            {
                Url = FailoverClient.Url,
                Database = FailoverClient.DefaultDatabase,
                TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate
            });

            SpinWait.SpinUntil(() => FailoverClient.DatabaseCommands.HasConflict(userDocumentId));

            using (var session = FailoverClient.OpenSession())
            {
                // Act
                var users = session.Advanced.LoadStartingWith<User>(usersPrefix);

                // Assert
                Assert.Equal(1, users.Length);
                Assert.Equal(users[0].Name, "AnotherName");
            }
        }

        private class TakeNewestConflictResolutionListener : IDocumentConflictListener
        {
            private static readonly string[] PropertiesToRemove =
            {
                "@id",
                "@etag",
                "Raven-Replication-Conflict",
                "Raven-Replication-Conflict-Document"
            };

            public bool TryResolveConflict(string key, JsonDocument[] conflictedDocs, out JsonDocument resolvedDocument)
            {
                var maxDate = conflictedDocs.Max(x => x.LastModified);
                resolvedDocument = conflictedDocs.FirstOrDefault(x => x.LastModified == maxDate);

                if (resolvedDocument == null) return false;

                foreach (var property in PropertiesToRemove) resolvedDocument.Metadata.Remove(property);
                return true;
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
