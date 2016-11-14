using System.IO;

namespace RavenDB.Unit.Tests.Extensions
{
    public static class DirectoryInfoExtensions
    {
        public static DirectoryInfo EnsureExists(this DirectoryInfo directoryInfo)
        {
            directoryInfo.Refresh();
            if (!directoryInfo.Exists) directoryInfo.Create();
            return directoryInfo;
        }
    }
}