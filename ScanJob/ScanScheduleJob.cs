using MongoDB.Bson;
using MongoDB.Driver;
using Quartz;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using SharpCompress.Common;

namespace ScanServices.ScanJob
{
    public class ScanScheduleJob : IJob
    {
        private static HashSet<string> processedFiles = new HashSet<string>();
        private readonly string folderPath = "C:\\Users\\Admin\\Desktop\\QuantzScan";
        private readonly string processedFolderPath = "C:\\Users\\Admin\\Desktop\\QuantzScan\\queues";

        public Task Execute(IJobExecutionContext context)
        {
            if (Directory.Exists(folderPath))
            {
                if (!Directory.Exists(processedFolderPath))
                {
                    Directory.CreateDirectory(processedFolderPath);
                }

                var files = Directory.GetFiles(folderPath);
                foreach (var file in files)
                {
                    if (!processedFiles.Contains(file))
                    {
                        var fileName = Path.GetFileName(file);

                        var destFile = Path.Combine(processedFolderPath, fileName);

                        try
                        {
                            File.Move(file, destFile);
                            Console.WriteLine($"File moved to: {destFile}");

                            processedFiles.Add(destFile);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"Error moving file {fileName}: {ex.Message}");
                        }
                    }
                    else
                    {
                        Console.WriteLine($"File already processed: {file}");
                    }
                }
            }
            else
            {
                Console.WriteLine($"Folder {folderPath} does not exist.");
            }
            return Task.CompletedTask;
        }

    }
}
