using System.Runtime.InteropServices;
using System.Security.AccessControl;
using System.Security.Principal;
using naivedb.core.configs;

namespace NaiveDB.Core.Utils
{
    public static class DbPathHelper
    {
        /// <summary>
        /// returns platform-consistent default for storage
        /// </summary>
        public static string GetDefaultDbPath(string appName = "naivedb")
        {
            string basePath;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                var programData = Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData);
                basePath = Path.Combine(programData, appName.ToLower()); // C:\ProgramData\naivedb
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                basePath = Path.Combine("/var/lib", appName.ToLower()); // /var/lib/naivedb
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                basePath = Path.Combine("/usr/local/var", appName.ToLower()); // /usr/local/var/naivedb
            }
            else
            {
                basePath = Path.Combine(Directory.GetCurrentDirectory(), appName.ToLower());
            }
            Directory.CreateDirectory(basePath);
            TrySetRestrictedPermissions(basePath);
            return basePath;
        }

        /// <summary>
        /// get default path for the db
        /// </summary>
        public static string GetDatabasePath(string dbName, string appName = "naivedb")
        {
            var basePath = GetDefaultDbPath(appName);
            var dbPath = Path.Combine(basePath, dbName);
            Directory.CreateDirectory(dbPath);
            return dbPath;
        }

        /// <summary>
        /// get default db info file path
        /// </summary>
        public static string GetDbInfoFilePath(string appName = "naivedb")
        {
            var dbOption = new DbOptions();
            return Path.Combine(GetDefaultDbPath(appName), dbOption.DbInfoFile);
        }

        private static void TrySetRestrictedPermissions(string path)
        {
            /*
                * windows:
                *   - current user: full control
                *   - administrators: full control
                *   - system: full control
                *   - other users: read & execute
                * 
                * linux/macOS:
                *   - owner: rwx
                *   - group: r-x
                *   - others: r-x

                ** todo: mssql style permissions -> on installation which is done by administrator privileges, create new windows service user and set permissions accordingly.
                mssql only allows that service user to have access to the data folder and do stuff, other users have no access at all
            */
            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    var dirInfo = new DirectoryInfo(path);
                    var dirSecurity = new DirectorySecurity();

                    var currentUser = WindowsIdentity.GetCurrent().User;
                    if (currentUser == null) 
                        return;

                    // current user (running process)
                    var currentUserRule = new FileSystemAccessRule(
                        currentUser,
                        FileSystemRights.FullControl,
                        InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                        PropagationFlags.None,
                        AccessControlType.Allow);

                    // administrators
                    var adminRule = new FileSystemAccessRule(
                        new SecurityIdentifier(WellKnownSidType.BuiltinAdministratorsSid, null),
                        FileSystemRights.FullControl,
                        InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                        PropagationFlags.None,
                        AccessControlType.Allow);

                    // system
                    var systemRule = new FileSystemAccessRule(
                        new SecurityIdentifier(WellKnownSidType.LocalSystemSid, null),
                        FileSystemRights.FullControl,
                        InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                        PropagationFlags.None,
                        AccessControlType.Allow);

                    // other users: read-only
                    var usersRule = new FileSystemAccessRule(
                        new SecurityIdentifier(WellKnownSidType.BuiltinUsersSid, null),
                        FileSystemRights.ReadAndExecute,
                        InheritanceFlags.ContainerInherit | InheritanceFlags.ObjectInherit,
                        PropagationFlags.None,
                        AccessControlType.Allow);

                    dirSecurity.SetAccessRuleProtection(true, false);
                    dirSecurity.AddAccessRule(currentUserRule);
                    dirSecurity.AddAccessRule(adminRule);
                    dirSecurity.AddAccessRule(systemRule);
                    dirSecurity.AddAccessRule(usersRule);

                    dirInfo.SetAccessControl(dirSecurity);
                }
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                        RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                {
                    // owner full (rwx), others read-only
                    var chmod = new System.Diagnostics.ProcessStartInfo
                    {
                        FileName = "chmod",
                        Arguments = "744 " + path,
                        UseShellExecute = false,
                        RedirectStandardOutput = true,
                        RedirectStandardError = true
                    };
                    System.Diagnostics.Process.Start(chmod)?.WaitForExit();
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Permission setup failed: " + ex.Message);
            }
        }


    }
}