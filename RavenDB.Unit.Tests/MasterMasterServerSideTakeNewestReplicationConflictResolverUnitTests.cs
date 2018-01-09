using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
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
    public class MasterMasterServerSideTakeNewestReplicationConflictResolverUnitTests : IDisposable
    {
        public MasterMasterServerSideTakeNewestReplicationConflictResolverUnitTests()
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

            var masterChanges = MasterClient.Changes();
            var failoverChanges = FailoverClient.Changes();
            using (var masterConflictObserver = new ConflictObserver())
            using (var failoverConflictObserver = new ConflictObserver())
            using (masterChanges.ForAllReplicationConflicts().Subscribe(masterConflictObserver))
            using (failoverChanges.ForAllReplicationConflicts().Subscribe(failoverConflictObserver))
            {
                masterChanges.WaitForAllPendingSubscriptions();
                failoverChanges.WaitForAllPendingSubscriptions();

                FailoverClient.EstablishReplicationTo(new ReplicationDestination
                {
                    Url = MasterClient.Url,
                    Database = MasterClient.DefaultDatabase,
                    TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate
                });

                MasterClient.EstablishReplicationTo(new ReplicationDestination
                {
                    Url = FailoverClient.Url,
                    Database = FailoverClient.DefaultDatabase,
                    TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate
                });

                masterConflictObserver.NewConflictEvent.WaitOne();
                failoverConflictObserver.NewConflictEvent.WaitOne();
            }

            using (var session = FailoverClient.OpenSession())
            {
                // Act
                var user = session.Load<User>(userDocumentId);

                // Assert
                Assert.Equal(user.Name, "AnotherName");
            }
        }

        [Fact]
        public async Task MultipleDocuments()
        {
            const int start = 3000;
            const int quantity = 1000;

            // Arrange
            Console.WriteLine("Establishing replication...");
            FailoverClient.EstablishReplicationTo(new ReplicationDestination
            {
                Url = MasterClient.Url,
                Database = MasterClient.DefaultDatabase,
                TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate
            });

            MasterClient.EstablishReplicationTo(new ReplicationDestination
            {
                Url = FailoverClient.Url,
                Database = FailoverClient.DefaultDatabase,
                TransitiveReplicationBehavior = TransitiveReplicationOptions.Replicate
            });

            // Act
            Console.WriteLine("Storing documents...");
            var insertInStoreATask = InsertDocumentsAsync(MasterClient, start, quantity);
            var insertInStoreBTask = InsertDocumentsAsync(FailoverClient, start, quantity);
            await TaskEx.WhenAll(insertInStoreATask, insertInStoreBTask);

            // Assert
            Console.WriteLine("Checking documents on master...");
            new ManualResetEvent(false).WaitOne();
            using (var session = MasterClient.OpenSession())
            {
                for (var i = start; i < start + quantity; i++)
                    session.Load<SomeDocument>($"SomeDocuments/{i}");
            }

            Console.WriteLine("Checking documents on failover...");
            using (var session = FailoverClient.OpenSession())
            {
                for (var i = start; i < start + quantity; i++)
                    session.Load<SomeDocument>($"SomeDocuments/{i}");
            }
        }

        private static async Task InsertDocumentsAsync(DocumentStore documentStore, int start, int quantity)
        {
            await TaskEx.Yield();

            for (int i = start; i < start + quantity; i++)
            {
                var document = new SomeDocument
                {
                    SomeInt = i,
                    SomeString = i.ToString()
                };

                await InsertDocumentAsync(documentStore, document);
            }
        }

        private static async Task InsertDocumentAsync(DocumentStore documentStore, SomeDocument document)
        {
            using (var session = documentStore.OpenAsyncSession())
            {
                await session.StoreAsync(document, $"SomeDocuments/{document.SomeInt}");
                await session.SaveChangesAsync();
            }
        }

        private class ConflictObserver : IObserver<ReplicationConflictNotification>, IDisposable
        {
            public AutoResetEvent NewConflictEvent { get; } = new AutoResetEvent(false);

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
        }

        private class User
        {
            public string Name { get; set; }
        }

        public class SomeDocument
        {
            public int SomeInt { get; set; }

            public string SomeString { get; set; }
        }
    }
}