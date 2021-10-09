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
using Alphaleonis.Win32.Vss;
using Alphaleonis.Win32.Filesystem;
using Ionic.Zip;
using System.Management;
using Cloudbase.ClusterSharedVolume;

namespace Cloudbase.Titan.HyperV.Backup
{
    public class BackupCancelledException : Exception
    {
        public BackupCancelledException()
            : base("Backup cancelled")
        {
        }
    }

    public enum EventAction 
    { 
        InitializingVSS, 
        StartingSnaphotSet, 
        SnapshotSetDone, 
        StartingArchive, 
        StartingEntry, 
        SavingEntry, 
        ArchiveDone, 
        DeletingSnapshotSet
    }

    public class BackupProgressEventArgs : EventArgs
    {
        public IDictionary<string, string> Components { get; set; }
        public string AcrhiveFileName { get; set; }
        public long BytesTransferred { get; set; }
        public bool Cancel { get; set; }
        public string CurrentEntry { get; set; }
        public int EntriesTotal { get; set; }
        public long TotalBytesToTransfer { get; set; }
        public EventAction Action { get; set; }
        public IDictionary<string, string> VolumeMap { get; set; }
    }

    public enum VMNameType { ElementName, SystemName }

    public class BackupManager
    {
        public event EventHandler<BackupProgressEventArgs> BackupProgress;

        private volatile bool cancel = false;

        public IDictionary<string, string> VSSBackup(IEnumerable<string> vmNames, VMNameType nameType, string backupOutputPath, 
                                                     string backupOutputFormat, bool singleSnapshot, int compressionLevel)
        {
            cancel = false;

            var vmNamesMap = GetVMNames(vmNames, nameType);

            if (vmNamesMap.Count > 0)
            {
                if (singleSnapshot)
                    BackupSubset(vmNamesMap, backupOutputPath, backupOutputFormat, compressionLevel);
                else
                    foreach (var kv in vmNamesMap)
                    {
                        var vmNamesMapSubset = new Dictionary<string, string>();
                        vmNamesMapSubset.Add(kv.Key, kv.Value);
                        BackupSubset(vmNamesMapSubset, backupOutputPath, backupOutputFormat, compressionLevel);
                    }               
            }

            return vmNamesMap;
        }

        private void BackupSubset(IDictionary<string, string> vmNamesMapSubset, string backupOutputPath, string backupOutputFormat, int compressionLevel)
        {
            IVssImplementation vssImpl = VssUtils.LoadImplementation();
            using (IVssBackupComponents vss = vssImpl.CreateVssBackupComponents())
            {
                RaiseEvent(EventAction.InitializingVSS, null, null);

                vss.InitializeForBackup(null);
                vss.SetBackupState(true, true, VssBackupType.Full, false);
                vss.SetContext(VssSnapshotContext.Backup);

                // Add Hyper-V writer
                Guid hyperVwriterGuid = new Guid("66841cd4-6ded-4f4b-8f17-fd23f8ddc3de");
                vss.EnableWriterClasses(new Guid[] { hyperVwriterGuid });

                using (IVssAsync async = vss.GatherWriterMetadata())
                    async.Wait();

                IList<IVssWMComponent> components = new List<IVssWMComponent>();
                // key: volumePath, value: volumeName. These values are equivalent on a standard volume, but differ in the CSV case  
                IDictionary<string, string> volumeMap = new Dictionary<string, string>();

                var wm = vss.WriterMetadata.Where((o) => o.WriterId.Equals(hyperVwriterGuid)).FirstOrDefault();
                foreach (var component in wm.Components)
                {
                    if (vmNamesMapSubset.ContainsKey(component.ComponentName))
                    {
                        components.Add(component);
                        vss.AddComponent(wm.InstanceId, wm.WriterId, component.Type, component.LogicalPath, component.ComponentName);
                        foreach (var file in component.Files)
                        {
                            string volumeName = null;
                            string volumePath = null;

                            if (CSV.IsSupported() && CSV.IsPathOnSharedVolume(file.Path))
                            {
                                CSV.ClusterPrepareSharedVolumeForBackup(file.Path, out volumePath, out volumeName);
                            }
                            else
                            {
                                volumePath = Path.GetPathRoot(file.Path).ToUpper();
                                volumeName = volumePath;
                            }

                            if (!volumeMap.ContainsKey(volumePath))
                                volumeMap.Add(volumePath, volumeName);
                        }
                    }
                }

                if (components.Count > 0)
                {
                    Guid vssSet = vss.StartSnapshotSet();

                    // Key: volumeName, value: snapshotGuid
                    IDictionary<string, Guid> snapshots = new Dictionary<string, Guid>();

                    foreach (var volumeName in volumeMap.Values)
                        snapshots.Add(volumeName, vss.AddToSnapshotSet(volumeName, Guid.Empty));

                    using (IVssAsync async = vss.PrepareForBackup())
                        async.Wait();

                    RaiseEvent(EventAction.StartingSnaphotSet, components, volumeMap);
                    using (IVssAsync async = vss.DoSnapshotSet())
                        async.Wait();
                    RaiseEvent(EventAction.SnapshotSetDone, components, volumeMap);

                    // key: volumeName, value: snapshotVolumePath 
                    IDictionary<string, string> snapshotVolumeMap = new Dictionary<string, string>();

                    foreach (var kv in snapshots)
                        snapshotVolumeMap.Add(kv.Key, vss.GetSnapshotProperties(kv.Value).SnapshotDeviceObject);

                    BackupFiles(backupOutputPath, backupOutputFormat, components, volumeMap, snapshotVolumeMap, vmNamesMapSubset, compressionLevel);

                    foreach (var component in components)
                        vss.SetBackupSucceeded(wm.InstanceId, wm.WriterId, component.Type, component.LogicalPath, component.ComponentName, true);

                    vss.BackupComplete();

                    RaiseEvent(EventAction.DeletingSnapshotSet, components, volumeMap);
                    vss.DeleteSnapshotSet(vssSet, true);
                }
            }
        }

        private void RaiseEvent(EventAction action, IList<IVssWMComponent> components, IDictionary<string, string> volumeMap)
        {
            if (BackupProgress != null)
            {
                var ebp = new BackupProgressEventArgs()
                {
                    Action = action
                };

                if (components != null)
                {
                    ebp.Components = new Dictionary<string, string>();
                    foreach (var component in components)
                        ebp.Components.Add(component.ComponentName, component.Caption);
                }

                if (volumeMap != null)
                {
                    ebp.VolumeMap = new Dictionary<string, string>();
                    foreach (var volume in volumeMap)
                        ebp.VolumeMap.Add(volume);
                }

                BackupProgress(this, ebp);
                if (ebp.Cancel)
                    throw new BackupCancelledException();
            }
        }

/*
        private static void BackupWriterMetadata(string backupPath, IVssBackupComponents vss, Guid vssSet)
        {
            foreach (var wm in vss.WriterMetadata)
            {
                string metadataFileName = Path.Combine(backupPath, string.Format("WriterMetadata_{0}_{1}.xml", wm.WriterId, vssSet));
                File.WriteAllText(metadataFileName, wm.SaveAsXml());
            }
        }
 */

        private void BackupFiles(string backupOutputPath, string backupOutputFormat, IList<IVssWMComponent> components, IDictionary<string, string> volumeMap, 
                                 IDictionary<string, string> snapshotVolumeMap,  IDictionary<string, string> vmNamesMap,
                                 int compressionLevel)
        {
            foreach (var component in components)
            {
                IList<System.IO.Stream> streams = new List<System.IO.Stream>();
                try
                {
                    string vmBackupPath = Path.Combine(backupOutputPath, string.Format(backupOutputFormat, vmNamesMap[component.ComponentName], component.ComponentName, DateTime.Now));
                    File.Delete(vmBackupPath);
                    using (ZipFile zf = new ZipFile(vmBackupPath))
                    {
                        if (compressionLevel < 0 || compressionLevel > 9)
                            throw new Exception("The provided compression level is not valid. The valid range is between 0 and 9.");

                        zf.ParallelDeflateThreshold = -1;
                        zf.SortEntriesBeforeSaving = true;
                        zf.TempFileFolder = backupOutputPath;
                        zf.UseZip64WhenSaving = Zip64Option.AsNecessary;
                        zf.CompressionLevel = (Ionic.Zlib.CompressionLevel)compressionLevel;

                        if (BackupProgress != null)
                        {
                            //zf.AddProgress += (sender, e) => { AddEventHandler(component, e); };
                            zf.SaveProgress += (sender, e) => { AddEventHandler(component, volumeMap, e); };
                        }

                        foreach (var file in component.Files)
                        {
                            string path;
                            if (file.IsRecursive)
                                path = file.Path;
                            else
                                path = Path.Combine(file.Path, file.FileSpecification);

                            // Get the longest matching path
                            var volumePath = volumeMap.Keys.OrderBy((o) => o.Length).Reverse().Where((o) => path.StartsWith(o, StringComparison.OrdinalIgnoreCase)).First();
                            var volumeName = volumeMap[volumePath];

                            AddPathToZip(snapshotVolumeMap[volumeName], volumePath.Length, zf, path, streams);
                        }

                        zf.Save();
                    }

                    if(cancel)
                        throw new BackupCancelledException();
                }
                finally
                {
                    // Make sure that all streams are closed
                    foreach (var s in streams)
                        s.Close();
                }
            }
        }

        private static void AddPathToZip(string snapshotPath, int volumePathLength,  /*IDictionary<string, string>  volumeMap, IDictionary<string, string> snapshotVolumeMap, */ZipFile zf, string vmPath, IList<System.IO.Stream> streams)
        {
            var srcPath = Path.Combine(snapshotPath, vmPath.Substring(volumePathLength));

            if (Directory.Exists(srcPath))
            {
                zf.AddDirectoryByName(vmPath);

                foreach (var srcChildPath in Directory.GetFileSystemEntries(srcPath))
                {
                    var srcChildPathRel = srcChildPath.Substring(snapshotPath.EndsWith(Path.PathSeparator) ? snapshotPath.Length : snapshotPath.Length + 1);
                    var childPath = Path.Combine(vmPath.Substring(0, volumePathLength), srcChildPathRel);
                    AddPathToZip(snapshotPath, volumePathLength, zf, childPath, streams);
                }
            }
            else if (File.Exists(srcPath))
            {
                var s = File.OpenRead(srcPath);
                streams.Add(s);
                // Cannot use AddFile with \\?\... snapshot paths
                zf.AddEntry(vmPath, s);
            }
            else
                throw new Exception(string.Format("Entry \"{0}\" not found in snapshot", srcPath));
        }

        protected bool UseWMIV2NameSpace
        {
            get
            {
                var version = Environment.OSVersion.Version;
                return version.Major >= 6 && version.Minor >= 2;
            }
        }

        protected string GetWMIScope(string host = "localhost")
        {
            string scopeFormatStr;
            if (UseWMIV2NameSpace)
                scopeFormatStr = "\\\\{0}\\root\\virtualization\\v2";
            else
                scopeFormatStr = "\\\\{0}\\root\\virtualization";

            return (string.Format(scopeFormatStr, host));
        }

        IDictionary<string, string> GetVMNames(IEnumerable<string> vmNames, VMNameType nameType)
        {
            IDictionary<string, string> d = new Dictionary<string, string>();

            string query;
            string vmIdField;

            if(UseWMIV2NameSpace)
            {
                query = "SELECT VirtualSystemIdentifier, ElementName FROM Msvm_VirtualSystemSettingData WHERE VirtualSystemType = 'Microsoft:Hyper-V:System:Realized'";
                vmIdField = "VirtualSystemIdentifier";
            }
            else
            {
                query = "SELECT SystemName, ElementName FROM Msvm_VirtualSystemSettingData WHERE SettingType = 3";
                vmIdField = "SystemName";
            }

            string inField = nameType == VMNameType.ElementName ? "ElementName" : vmIdField;

            ManagementScope scope = new ManagementScope(GetWMIScope());

            if(vmNames != null && vmNames.Count() > 0)
                query += string.Format(" AND ({0})", GetORStr(inField, vmNames));

            using (ManagementObjectSearcher searcher = new ManagementObjectSearcher(scope, new ObjectQuery(query)))
            {
                using (var moc = searcher.Get())
                    foreach(var mo in moc)
                        using(mo)
                            d.Add((string)mo[vmIdField], (string)mo["ElementName"]);
            }

            return d;
        }

        private static string GetORStr(string fieldName, IEnumerable<string> vmNames)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var vmName in vmNames)
            {
                if (sb.Length > 0)
                    sb.Append(" OR ");
                sb.Append(string.Format("{0} = '{1}'", fieldName, EscapeWMIStr(vmName)));
            }
            return sb.ToString();
        }

        private static string EscapeWMIStr(string str)
        {
            return str != null ? str.Replace("'", "''") : null;
        }

        private void AddEventHandler(IVssWMComponent component, IDictionary<string, string> volumeMap, ZipProgressEventArgs e)
        {
            BackupProgressEventArgs ebp = null;

            if (e.EventType == ZipProgressEventType.Saving_Started)
            {
                ebp = new BackupProgressEventArgs()
                {
                    AcrhiveFileName = e.ArchiveName,
                    Action = EventAction.StartingArchive
                };
            }
            else if (e.EventType == ZipProgressEventType.Saving_BeforeWriteEntry ||
                     e.EventType == ZipProgressEventType.Saving_EntryBytesRead)
            {
                var action = e.EventType == ZipProgressEventType.Saving_BeforeWriteEntry ? EventAction.StartingEntry : EventAction.SavingEntry;

                ebp = new BackupProgressEventArgs()
                {
                    AcrhiveFileName = e.ArchiveName,
                    Action = action,
                    CurrentEntry = e.CurrentEntry.FileName,
                    EntriesTotal = e.EntriesTotal,
                    TotalBytesToTransfer = e.TotalBytesToTransfer,
                    BytesTransferred = e.BytesTransferred
                };
            }
            else if (e.EventType == ZipProgressEventType.Saving_Completed)
            {
                ebp = new BackupProgressEventArgs()
                {
                    AcrhiveFileName = e.ArchiveName,
                    Action = EventAction.ArchiveDone
                };
            }

            if (ebp != null)
            {
                ebp.Components = new Dictionary<string, string>();
                ebp.Components.Add(component.ComponentName, component.Caption);

                ebp.VolumeMap = new Dictionary<string, string>();
                foreach (var volume in volumeMap)
                    ebp.VolumeMap.Add(volume.Key, volume.Value);

                BackupProgress(this, ebp);

                //if (ebp.Cancel)
                //    throw new BackupCancelledException();

                // Close the zip file operation neatly and throw the exception afterwards
                e.Cancel = ebp.Cancel;
                cancel = e.Cancel;
            }
        }
    }
}
