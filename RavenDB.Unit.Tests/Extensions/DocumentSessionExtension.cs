using Raven.Client;

namespace RavenDB.Unit.Tests.Extensions
{
    public static class DocumentSessionExtension
    {
        public static T LoadOrStoreNew<T>(this IDocumentSession session, string id) where T : new()
        {
            var entity = session.Load<T>(id);
            if (entity == null) session.Store(entity = new T());
            return entity;
        }
    }
}