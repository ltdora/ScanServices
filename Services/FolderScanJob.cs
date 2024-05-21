using Microsoft.Extensions.Options;
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
using Microsoft.Extensions.Configuration;

namespace ScanServices.Scan
{
    public class FolderScanJob : IJob
    {
        private static HashSet<string> processedFiles = new HashSet<string>();
        private readonly string folderPath = "C:\\Users\\Admin\\Desktop\\QuantzScan";
        private readonly string processedFolderPath = "C:\\Users\\Admin\\Desktop\\QuantzScan\\readed";

        private readonly IMongoCollection<BsonDocument> _collection;

        public FolderScanJob()
        {
            var client = new MongoClient("mongodb+srv://datlt:Laitiendat1312.@helloworld.bbqg5uv.mongodb.net/?retryWrites=true&w=majority&appName=HelloWorld");
            var database = client.GetDatabase("HelloMongo");
            _collection = database.GetCollection<BsonDocument>("Users");
        }

        public Task Execute(IJobExecutionContext context)
        {
            if (Directory.Exists(folderPath))
            {
                // Tạo thư mục đích nếu chưa tồn tại
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
                        Console.WriteLine($"Read file: {fileName}");

                        // Đọc nội dung file
                        string[] content = File.ReadAllLines(file);
                        string field = content[0];
                        string value = content[1];

                        var document = new BsonDocument { { field, value } };
                        Console.WriteLine(document);

                        //_collection.InsertOneAsync(document);

                        var getall = _collection.Find(_ => true).ToList();
                        foreach (var item in getall)
                        {
                            Console.WriteLine(item);
                        }

                        Console.WriteLine($"Saved {fileName} to Database");

                        // Di chuyển file sau khi đã xử lý xong
                        var destFile = Path.Combine(processedFolderPath, fileName);

                        try
                        {
                            File.Move(file, destFile);
                            Console.WriteLine($"File moved to: {destFile}");

                            // Thêm file vào danh sách đã xử lý
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
