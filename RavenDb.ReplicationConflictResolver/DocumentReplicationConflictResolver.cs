using System;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Json.Linq;

namespace RavenDb.ReplicationConflictResolver
{
    public class DocumentReplicationConflictResolver : AbstractDocumentReplicationConflictResolver
    {
        public DocumentReplicationConflictResolver()
        {
        }

        public override bool TryResolve(string id, RavenJObject metadata, RavenJObject document, JsonDocument existingDoc, Func<string, JsonDocument> getDocument)
        {
            return true;
        }
    }
}
