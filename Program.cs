using MongoDB.Bson;
using MongoDB.Driver;
using Quartz.Impl;
using Quartz;
using System;
using System.Threading.Tasks;
using ScanServices.ScanJob;
using System.IO;
using System.Collections.Generic;
using System.Linq;

namespace ScanServices
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            Task task1 = Task.Run(() => ScanQueueFolder());
            Task task2 = Task.Run(() => ScanSchedule());

            await Task.WhenAll(task1, task2);
        }
        static async Task ScanSchedule()
        {
            IScheduler scheduler = await StdSchedulerFactory.GetDefaultScheduler();
            await scheduler.Start();

            IJobDetail job = JobBuilder.Create<ScanScheduleJob>()
                .WithIdentity("folderScanJob", "group1")
                .Build();
            ITrigger trigger = TriggerBuilder.Create()
                .WithIdentity("trigger1", "group1")
                .StartNow()
                .WithSimpleSchedule(x => x
                    .WithIntervalInSeconds(3)
                    //.WithIntervalInMinutes(5)
                    .RepeatForever())
                .Build();

            await scheduler.ScheduleJob(job, trigger);

            Console.WriteLine("Press [Enter] to close the application.");
            Console.ReadLine();

            await scheduler.Shutdown();
        }
        static void ScanQueueFolder()
        {
            string folderPath = @"C:\Users\Admin\Desktop\QuantzScan\queues";

            if (!Directory.Exists(folderPath))
            {
                Console.WriteLine("Folder does not exist");
                return;
            }

            FileSystemWatcher watcher = new FileSystemWatcher
            {
                Path = folderPath,
                NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite,
                Filter = "*.*"
            };

            watcher.Created += ScanJob.ScanQueue.ReadFileCreated;

            watcher.EnableRaisingEvents = true;

            Console.ReadLine();
        }
    }
}
