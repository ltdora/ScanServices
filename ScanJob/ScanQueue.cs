using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ScanServices.ScanJob
{
    public static class ScanQueue
    {
        public static async void ReadFileCreated(object sender, FileSystemEventArgs e)
        {
            HashSet<string> processedFiles = new HashSet<string>();
            string folderPath = "C:\\Users\\Admin\\Desktop\\QuantzScan\\queues";
            string processedFolderPath = "C:\\Users\\Admin\\Desktop\\QuantzScan\\readed";
            IMongoCollection<BsonDocument> _collection;

            var client = new MongoClient("mongodb+srv://datlt:Laitiendat1312.@helloworld.bbqg5uv.mongodb.net/?retryWrites=true&w=majority&appName=HelloWorld");
            var database = client.GetDatabase("HelloMongo");
            _collection = database.GetCollection<BsonDocument>("CDRs");

            try
            {
                var files = Directory.GetFiles(folderPath);
                foreach (var file in files)
                {
                    if (!processedFiles.Contains(file))
                    {
                        var fileName = Path.GetFileName(file);
                        Console.WriteLine($"Read file: {fileName}");

                        string[] lines = File.ReadAllLines(file);

                        int rows = lines.Length;
                        int cols = lines.First().Split(',').Length;

                        string[,] csvData = new string[rows, cols];

                        for (int i = 0; i < rows; i++)
                        {
                            string[] lineData = lines[i].Split(',');
                            for (int j = 0; j < cols; j++)
                            {
                                csvData[i, j] = lineData[j];
                            }
                        }

                        string[] fields = new string[cols];
                        for (int i = 0; i < cols; i++)
                        {
                            fields[i] = csvData[0, i];
                        }

                        var document = new BsonDocument();

                        var allDocument = new List<BsonDocument>();

                        for (int i = 1; i < rows; i++)
                        {
                            for (int j = 0; j < cols; j++)
                            {
                                var mergedoc = new BsonDocument { { fields[j], csvData[i, j] } };
                                document.Merge(mergedoc, true);
                            }
                            allDocument.Add(document);
                            document = new BsonDocument();
                        }
                        _collection.InsertMany(allDocument);

                        //var count = _collection.EstimatedDocumentCount();

                        int count = 0;

                        var getall = _collection.Find(_ => true).ToList();
                        foreach (var item in getall)
                        {
                            count++;
                            //Console.WriteLine(item);
                        }

                        Console.WriteLine($"Number of document: {count}");

                        Console.WriteLine($"Saved {fileName} to Database");

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
            catch (Exception ex)
            {
                Console.WriteLine($"Error reading file: {ex.Message}");
            }
        }
    }
}
