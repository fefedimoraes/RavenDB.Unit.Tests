using System;
using System.IO;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Abstractions.Replication;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using RavenDb.ReplicationConflictResolver;
using RavenDB.Unit.Tests.Extensions;
using Xunit;

namespace RavenDB.Unit.Tests
{
    public class ServerSideTakeNewestReplicationConflictResolverUnitTests : IDisposable
    {
        public ServerSideTakeNewestReplicationConflictResolverUnitTests()
        {
            const int masterServerPort = 9900;
            const int failoverServerPort = 9901;
            const string defaultDatabase = "ReplicatingDatabase";
            const string loopbackHttpAddress = "http://127.0.0.1";

            var conflictResolverPluginName = typeof(TakeNewestConflictResolver).Assembly.GetName().Name + ".dll";

            PluginsDirectory = new DirectoryInfo("RavenPlugins").EnsureExists();
            File.Copy(conflictResolverPluginName, Path.Combine(PluginsDirectory.Name, conflictResolverPluginName), true);

            MasterServer = new EmbeddableDocumentStore
            {
                RunInMemory = true,
                UseEmbeddedHttpServer = true,
                Configuration =
                {
                    Port = masterServerPort,
                    PluginsDirectory = PluginsDirectory.Name,
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
                    PluginsDirectory = PluginsDirectory.Name,
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

            MasterServer.Initialize();
            FailoverServer.Initialize();
            MasterClient.Initialize();
            FailoverClient.Initialize();

            MasterClient.ExecuteIndex(new RavenDocumentsByEntityName());
            FailoverClient.ExecuteIndex(new RavenDocumentsByEntityName());
        }

        public DirectoryInfo PluginsDirectory { get; set; }

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

            PluginsDirectory.Delete(true);
        }

        [Fact(DisplayName = "On loading, conflicts should be handled in the server side")]
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

            var changes = FailoverClient.Changes();
            using (var conflictObserver = new ConflictObserver())
            using (changes.ForAllReplicationConflicts().Subscribe(conflictObserver))
            {
                changes.WaitForAllPendingSubscriptions();
                MasterClient.EstablishReplicationTo(new ReplicationDestination
                {
                    Url = FailoverClient.Url,
                    Database = FailoverClient.DefaultDatabase,
                    TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate
                });
                conflictObserver.WaitOne();
            }

            using (var session = FailoverClient.OpenSession())
            {
                // Act
                var user = session.Load<User>(userDocumentId);

                // Assert
                Assert.Equal(user.Name, "AnotherName");
            }
        }

        private class ConflictObserver : IObserver<ReplicationConflictNotification>, IDisposable
        {
            private AutoResetEvent NewConflictEvent { get; } = new AutoResetEvent(false);

            public void OnNext(ReplicationConflictNotification value)
            {
                NewConflictEvent.Set();
            }

            public void OnError(Exception error)
            {
            }

            public void OnCompleted()
            {
            }

            public void Dispose()
            {
                NewConflictEvent.Dispose();
            }

            public void WaitOne()
            {
                NewConflictEvent.WaitOne();
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}