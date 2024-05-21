using Quartz.Impl;
using Quartz;
using System;
using System.Threading.Tasks;
using ScanServices.Scan;
using MongoDB.Bson;
using MongoDB.Driver;

namespace ScanServices
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            var client = new MongoClient("mongodb+srv://datlt:Laitiendat1312.@helloworld.bbqg5uv.mongodb.net/?retryWrites=true&w=majority&appName=HelloWorld");
            var database = client.GetDatabase("HelloMongo");
            var collection = database.GetCollection<BsonDocument>("Users");

            // Cấu hình Quartz.NET
            IScheduler scheduler = await StdSchedulerFactory.GetDefaultScheduler();
            await scheduler.Start();

            // Định nghĩa công việc và liên kết với lớp FolderScanJob
            IJobDetail job = JobBuilder.Create<FolderScanJob>()
                .WithIdentity("folderScanJob", "group1")
                .Build();

            // Tạo lịch trình để chạy công việc
            ITrigger trigger = TriggerBuilder.Create()
                .WithIdentity("trigger1", "group1")
                .StartNow()
                .WithSimpleSchedule(x => x
                    .WithIntervalInSeconds(3) // Chạy mỗi 3 giây
                    .RepeatForever())
                .Build();

            // Liên kết công việc và lịch trình với Scheduler
            await scheduler.ScheduleJob(job, trigger);

            // Giữ ứng dụng console chạy
            Console.WriteLine("Press [Enter] to close the application.");
            Console.ReadLine();

            await scheduler.Shutdown();
        }
    }
}
