using System;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Json.Linq;
using static Raven.Abstractions.Data.Constants;

namespace RavenDb.ReplicationConflictResolver
{
    /// <summary>
    /// An <see cref="AbstractDocumentReplicationConflictResolver"/>
    /// that resolves conflicts taking the newest document between two conflicting documents.
    /// </summary>
    public class TakeNewestConflictResolver : AbstractDocumentReplicationConflictResolver
    {
        public TakeNewestConflictResolver()
        {
            
        }

        /// <inheritdoc/>
        public override bool TryResolve(string id, RavenJObject metadata, RavenJObject document,
            JsonDocument existingDoc, Func<string, JsonDocument> getDocument)
        {
            if (ShouldReplaceWithDocument(metadata, existingDoc))
            {
                metadata.ReplacePropertiesWith(existingDoc.Metadata);
                document.ReplacePropertiesWith(existingDoc.DataAsJson);
            }

            return true;
        }

        /// <summary>
        /// Checks if the provided <paramref name="document"/>
        /// should replace the document respective to the provided <paramref name="metadata"/>.
        /// </summary>
        /// <param name="metadata">
        /// The <see cref="RavenJObject"/> that represents the metadata of the document being replaced.
        /// </param>
        /// <param name="document">
        /// The <see cref="JsonDocument"/> that will replace a document.
        /// </param>
        /// <returns>
        /// <c>true</c> if the provided <paramref name="document"/> should replace the document; otherwise, <c>false</c>.
        /// </returns>
        private static bool ShouldReplaceWithDocument(RavenJObject metadata, JsonDocument document) =>
            document != null &&
            !document.IsConflicted() &&
            !metadata.IsSameOrNewerThan(document);
    }

    /// <summary>
    /// Contains extension methods for <see cref="JsonDocument"/>.
    /// </summary>
    public static class JsonDocumentExtensions
    {
        /// <summary>
        /// Checks if the provided <paramref name="document"/> is conflicted due to replications.
        /// </summary>
        /// <param name="document">A <see cref="JsonDocument"/> to be checked.</param>
        /// <returns><c>true</c> if the provided <paramref name="document"/> is conflicted; otherwise, <c>false</c>.</returns>
        public static bool IsConflicted(this JsonDocument document) =>
            document.Metadata[RavenReplicationConflict] != null;
    }

    /// <summary>
    /// Contains extension methods for <see cref="RavenJObject"/>.
    /// </summary>
    public static class RavenJObjectExtensions
    {
        /// <summary>
        /// Gets the time of the last modification occurred in the provided <paramref name="metadata"/>.
        /// </summary>
        /// <param name="metadata">A <see cref="RavenJObject"/> that represents a document metadata.</param>
        /// <returns>
        /// A <see cref="DateTime"/> that represents the time of the last modification occurred
        /// in the provided <paramref name="metadata"/>, if the data exists; otherwise, <c>null</c>.
        /// </returns>
        public static DateTime? GetLastModified(this RavenJObject metadata) => metadata[LastModified]
            ?.Value<DateTime?>();

        /// <summary>
        /// Checks if the provided <paramref name="metadata"/>
        /// was last modified at the same time or later than the provided <paramref name="document"/>.
        /// </summary>
        /// <param name="metadata">
        /// A <see cref="RavenJObject"/> that represents a document metadata.
        /// </param>
        /// <param name="document">
        /// The <see cref="JsonDocument"/> to compare with the provided <paramref name="metadata"/>.
        /// </param>
        /// <returns>
        /// <c>true</c> if the last modified from the provided <paramref name="metadata"/>
        /// is same or later than the <see cref="JsonDocument.LastModified"/> of the provided <paramref name="document"/>.
        /// </returns>
        public static bool IsSameOrNewerThan(this RavenJObject metadata, JsonDocument document)
        {
            if (!document.LastModified.HasValue) return true;

            var metadataLastModified = metadata.GetLastModified();
            if (!metadataLastModified.HasValue) return false;

            return metadataLastModified >= document.LastModified;
        }

        /// <summary>
        /// Replaces all <see cref="RavenJObject.Properties"/> from the provided <paramref name="target"/>
        /// with the <see cref="RavenJObject.Properties"/> from the provided <paramref name="source"/>.
        /// </summary>
        /// <param name="target">The <see cref="RavenJObject"/> to have its properties replaced.</param>
        /// <param name="source">The <see cref="RavenJObject"/> that provides the new properties.</param>
        /// <returns>A reference to the provided <paramref name="target"/>.</returns>
        public static RavenJObject ReplacePropertiesWith(this RavenJObject target, RavenJObject source)
        {
            foreach (var key in target.Keys)
                target.Remove(key);

            foreach (var property in source)
                target.Add(property.Key, property.Value);

            return target;
        }
    }
}