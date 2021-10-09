/*
 *  Copyright 2012 Cloudbase Solutions Srl
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU Lesser General Public License as published by
 *  the Free Software Foundation; either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301  USA 
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CommandLine;
using System.IO;
using CommandLine.Text;
using System.Threading;
using System.Diagnostics;

namespace Cloudbase.Titan.HyperV.Backup.CLI
{
    class Program
    {
        static volatile bool cancel = false;
        static int currentWidth = 0;
        static int consoleWidth = 0;

        class Options : CommandLineOptionsBase
        {
            [ValueList(typeof(List<string>), MaximumElements = 0)]
            public IList<string> items = null;

            [Option("b", "backup", HelpText = "Perform a backup (default).", MutuallyExclusiveSet = "br")]
            public bool backup = true;

            [Option("r", "restore", HelpText = "Perform a restore.", MutuallyExclusiveSet = "br")]
            public bool restore = false;

            [Option("f", "file",  HelpText = "Text file containing a list of VMs to backup, one per line.", MutuallyExclusiveSet = "fla")]
            public string file = null;

            [OptionList("l", "list", Separator = ',', HelpText = "List of VMs to backup, comma separated.", MutuallyExclusiveSet = "fla")]
            public IList<string> list = null;

            [Option("a", "all", HelpText = "backup all VMs on this server (default).", MutuallyExclusiveSet = "fla")]
            public bool all = true;

            [Option("n", "name", HelpText = "If set, VMs to backup are specified by name (default).", MutuallyExclusiveSet="ng")]
            public bool name = true;

            [Option("g", "guid", HelpText = "If set, VMs to backup are specified by guid.", MutuallyExclusiveSet = "ng")]
            public bool guid = false;

            [Option("o", "output", Required = true, HelpText = "Backup ouput folder.")]
            public string output = null;

            [Option(null, "outputformat", HelpText = "Backup archive name format. {0} is the VM's name, {1} the VM's GUID and {2} is the current date and time. Default: \"{0}_{2:yyyyMMddHHmmss}.zip\"")]
            public string outputFormat = "{0}_{2:yyyyMMddHHmmss}.zip";

            [Option("s", "singlevss", HelpText = "Perform one single snapshot for all the VMs.")]
            public bool singleSnapshot = false;

            [Option(null, "compressionlevel", HelpText = "Compression level, between 0 (no compression) and 9 (max. compression). Default: 6")]
            public int compressionLevel = 6;

            [HelpOption(HelpText = "Display this help screen.")]
            public string GetUsage()
            {
                var help = new HelpText(Environment.NewLine);
                help.AdditionalNewLineAfterOption = true;
                HandleParsingErrorsInHelp(help);
                help.AddOptions(this);

                return help.ToString();
            }

            private void HandleParsingErrorsInHelp(HelpText help)
            {
                string errors = help.RenderParsingErrorsText(this);
                if (!string.IsNullOrEmpty(errors))
                    help.AddPreOptionsLine(string.Concat("ERROR: ", errors, Environment.NewLine));
            }
        }

        static void Main(string[] args)
        {
            try
            {
                Stopwatch stopwatch = new Stopwatch();
                stopwatch.Start();

                Console.WriteLine("Cloudbase HyperVBackup 1.0 beta1");
                Console.WriteLine("Copyright (C) 2012 Cloudbase Solutions Srl");
                Console.WriteLine("http://www.cloudbasesolutions.com");

                var options = new Options();
                ICommandLineParser parser = new CommandLineParser();
                if (!parser.ParseArguments(args, options, Console.Out))
                    Environment.Exit(1);

                // TODO :-)
                if (options.restore)
                    throw new NotImplementedException();

                GetConsoleWidth();

                var vmNames = GetVMNames(options);

                Console.WriteLine();
                if (vmNames == null)
                    Console.WriteLine("Backing up all VMs on this server");

                if (!Directory.Exists(options.output))
                    throw new Exception(string.Format("The folder \"{0}\" is not valid", options.output));

                VMNameType nameType = options.name ? VMNameType.ElementName : VMNameType.SystemName;

                BackupManager mgr = new BackupManager();
                mgr.BackupProgress += new EventHandler<BackupProgressEventArgs>(mgr_BackupProgress);

                Console.CancelKeyPress += new ConsoleCancelEventHandler(Console_CancelKeyPress);

                var vmNamesMap = mgr.VSSBackup(vmNames, nameType, options.output, options.outputFormat, options.singleSnapshot, options.compressionLevel);

                CheckRequiredVMs(vmNames, nameType, vmNamesMap);

                ShowElapsedTime(stopwatch);
            }
            catch (BackupCancelledException ex)
            {
                Console.Error.WriteLine(string.Format(ex.Message));
                Environment.Exit(3);
            }
            catch (Exception ex)
            {
                Console.Error.WriteLine(string.Format("Error: {0}", ex.Message));
                Console.Error.WriteLine(ex.StackTrace);
                Environment.Exit(2);
            }
          
            Environment.Exit(cancel ? 3 : 0);
        }

        private static void ShowElapsedTime(Stopwatch stopwatch)
        {
            stopwatch.Stop();
            var ts = stopwatch.Elapsed;
            Console.WriteLine();
            Console.WriteLine(string.Format("Elapsed time: {0:00}:{1:00}:{2:00}.{3:000}", ts.Hours, ts.Minutes, ts.Seconds, ts.Milliseconds));
        }

        private static void CheckRequiredVMs(IEnumerable<string> vmNames, VMNameType nameType, IDictionary<string, string> vmNamesMap)
        {
            if (vmNames != null)
                foreach (var vmName in vmNames)
                    if (nameType == VMNameType.SystemName && !vmNamesMap.Keys.Contains(vmName, StringComparer.OrdinalIgnoreCase) ||
                       nameType == VMNameType.ElementName && !vmNamesMap.Values.Contains(vmName, StringComparer.OrdinalIgnoreCase))
                    {
                        Console.WriteLine(string.Format("WARNING: \"{0}\" not found", vmName));
                    }
        }

        private static void GetConsoleWidth()
        {
            try
            {
                consoleWidth = Console.WindowWidth;
            }
            catch (Exception)
            {
                consoleWidth = 80;
            }
        }

        private static IEnumerable<string> GetVMNames(Options options)
        {
            IEnumerable<string> vmNames = null;

            if (options.file != null)
                vmNames = File.ReadAllLines(options.file);
            else if (options.list != null)
                vmNames = options.list;

            if (vmNames != null)
                vmNames = (from o in vmNames where o.Trim().Length > 0 select o.Trim());

            return vmNames;
        }

        static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            Console.Error.WriteLine();
            Console.Error.WriteLine("Cancelling backup...");

            // Avoid CTRL+C during VSS snapshots
            cancel = true;
            e.Cancel = true;
        }

        static void mgr_BackupProgress(object sender, BackupProgressEventArgs e)
        {
            switch (e.Action)
            {
                case EventAction.InitializingVSS:
                    Console.WriteLine("Initializing VSS");
                    break;
                case EventAction.StartingSnaphotSet:
                    Console.WriteLine();
                    Console.WriteLine("Starting snapshot set for:");
                    foreach (var componentName in e.Components.Values)
                        Console.WriteLine(componentName);
                    Console.WriteLine();
                    Console.WriteLine("Volumes:");
                    foreach (var volumePath in e.VolumeMap.Keys)
                        Console.WriteLine(volumePath);
                    break;
                case EventAction.DeletingSnapshotSet:
                    Console.WriteLine("Deleting snapshot set");
                    break;
                case EventAction.StartingArchive:
                    Console.WriteLine();
                    foreach (var componentName in e.Components.Values)
                        Console.WriteLine(string.Format("Component: \"{0}\"", componentName));
                    Console.WriteLine(string.Format("Archive: \"{0}\"", e.AcrhiveFileName));
                    break;
                case EventAction.StartingEntry:
                    Console.WriteLine(string.Format("Entry: \"{0}\"", e.CurrentEntry));
                    currentWidth = 0;
                    break;
                case EventAction.SavingEntry:
                    if (e.TotalBytesToTransfer > 0)
                    {                      
                        int width = (int)Math.Round(e.BytesTransferred * consoleWidth / (decimal)e.TotalBytesToTransfer);

                        for (int i = 0; i < width - currentWidth; i++)
                            Console.Write(".");
                        currentWidth = width;

                        if (e.BytesTransferred == e.TotalBytesToTransfer)
                            Console.WriteLine();

                        //Console.WriteLine(string.Format("{0:0.#}%", e.BytesTransferred * 100 / (decimal)e.TotalBytesToTransfer));
                    }
                    break;
            }

            e.Cancel = cancel;
        }
    }
}
