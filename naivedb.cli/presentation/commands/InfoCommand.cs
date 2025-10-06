using naivedb.cli.presentation.constants;
using Spectre.Console;

namespace naivedb.cli.presentation.commands
{
    public class InfoCommand : ICommand
    {
        public Task ExecuteAsync(string[] args)
        {
            AnsiConsole.Write(new FigletText("System Info")
                .LeftJustified()
                .Color(Color.Green));
            
            var infoTable = new Table()
                .Border(TableBorder.Rounded)
                .Title("System Information")
                .AddColumn("Component")
                .AddColumn("Version/Info");

            infoTable.AddRow("naiveDB Version", AppConstants.Version);
            infoTable.AddRow(".NET Runtime", Environment.Version.ToString());
            infoTable.AddRow("OS Platform", Environment.OSVersion.Platform.ToString());
            infoTable.AddRow("OS Version", Environment.OSVersion.VersionString);
            infoTable.AddRow("Machine Name", Environment.MachineName);
            infoTable.AddRow("Working Directory", Environment.CurrentDirectory);

            AnsiConsole.Write(infoTable);
            AnsiConsole.WriteLine();
            
            var drivesPanel = new Panel(GetDrivesInfo())
            {
                Header = new PanelHeader("Storage Information", Justify.Center),
                Border = BoxBorder.Rounded
            };

            AnsiConsole.Write(drivesPanel);

            return Task.CompletedTask;
        }

        private string GetDrivesInfo()
        {
            var drives = DriveInfo.GetDrives()
                .Where(d => d.IsReady)
                .Select(d => $"[yellow]{d.Name}[/] - {d.AvailableFreeSpace / (1024 * 1024 * 1024):F1} GB free of {d.TotalSize / (1024 * 1024 * 1024):F1} GB")
                .ToArray();

            return string.Join("\n", drives);
        }
    }
}