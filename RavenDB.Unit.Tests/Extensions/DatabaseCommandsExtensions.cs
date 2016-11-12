using Raven.Client.Connection;
using Raven.Client.Exceptions;

namespace RavenDB.Unit.Tests.Extensions
{
    public static class DatabaseCommandsExtensions
    {
        public static bool HasConflict(this IDatabaseCommands databaseCommands, string id)
        {
            try
            {
                return databaseCommands.Head(id).Metadata.ContainsKey("Raven-Replication-Conflict");
            }
            catch (ConflictException)
            {
                return true;
            }
        }
    }
}