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
        public static void ReadFileCreated(object sender, FileSystemEventArgs e)
        {
            HashSet<string> processedFiles = new HashSet<string>();
            string folderPath = "C:\\Users\\Admin\\Desktop\\QuantzScan\\queues";
            string processedFolderPath = "C:\\Users\\Admin\\Desktop\\QuantzScan";
            IMongoCollection<BsonDocument> _CDRscollection;
            IMongoCollection<BsonDocument> _CDRsLogcollection;

            //var client = new MongoClient("mongodb+srv://datlt:Laitiendat1312.@helloworld.bbqg5uv.mongodb.net/?retryWrites=true&w=majority&appName=HelloWorld");
            //var database = client.GetDatabase("HelloMongo");

            var client = new MongoClient("mongodb://localhost:27017/");
            var database = client.GetDatabase("CDRsReport");

            _CDRscollection = database.GetCollection<BsonDocument>("CDRs");
            _CDRsLogcollection = database.GetCollection<BsonDocument>("CDRsLog");

            try
            {
                var files = Directory.GetFiles(folderPath);
                foreach (var file in files)
                {
                    if (!processedFiles.Contains(file))
                    {
                        try
                        {
                            var time = DateTime.Now;
                            string TimeStart = time.ToString("yyyy-MM-dd HH:mm:ss");
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

                            string[] fields = new string[] { "CallStart_ms", "CallStart", "CallEnd_ms", "CallEnd",
                                "Duration", "Acc_AccountID", "Acc_AddressID", "Acc_TenantID", "Acc_Number", "Acc_Tenant",
                                "Acc_Name", "Acc_Address", "Acc_AddressPublic", "Acc_AddressCombined", "Orig_Number", "Dest_Name",
                                "Dest_Number", "Dest_Type", "Dest_Tenant", "Dest_TenantID", "PricelistID", "PricelistVersion", "PricelistTable",
                                "Tariff", "PostRating", "ChargeAccount", "ChargeTenant", "ChargeSystem", "CallLeg", "Orig_IP", "Dest_IP", "CdrID",
                                "CallID", "Alert_ms", "Alert_seconds", "Orig_Gateway", "Dest_Gateway", "Pres_Preferred", "Pres_Asserted", "Cause",
                                "Flags", "Scope", "Acc_NumberPrivate", "CallType", "BillingInfo", "SIPCall-ID", "Q_850Cause", "Dest_Acc_ID",
                                "Dest_Acc_Name", "Dest_Addr_ID", "Dest_Addr_Number", "OutboundDest_" };
                            
                            //for (int i = 0; i < cols; i++)
                            //{
                            //    fields[i] = csvData[0, i].Replace(" ","");
                            //}

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
                            _CDRscollection.InsertMany(allDocument);

                            time = DateTime.Now;
                            string TimeEnd = time.ToString("yyyy-MM-dd HH:mm:ss");

                            int numberofrecord = allDocument.Count;

                            //string[] fieldslog = new string[] { "TypeWork", "FileName", "TimeStartRecord",
                            //"TimeEndRecord", "NumberOfRecord", "TotalNumberOfRecord", "Status", "Error" };

                            var totalrecord = _CDRscollection.EstimatedDocumentCount();

                            var recordlogdocument = new BsonDocument() { { "TypeWork", "Write" }, { "FileName", fileName },
                                { "TimeStartRecord", TimeStart }, { "TimeEndRecord", TimeEnd }, { "NumberOfRecord" , numberofrecord},
                                {"TotalNumberOfRecord", totalrecord }, { "Status", "Success" } };

                            _CDRsLogcollection.InsertOne(recordlogdocument);

                            Console.WriteLine($"Number of document: {totalrecord}");
                            Console.WriteLine($"Saved {fileName} to Database");

                            DisposeClient(client);
                            static void DisposeClient(MongoClient client)
                            {
                                if (client != null)
                                {
                                    client = null;
                                    Console.WriteLine("Disconnected to MongoDB.");
                                }
                            }

                            var destFile = Path.Combine(processedFolderPath, fileName);
                            try
                            {
                                File.Move(file, destFile);
                                Console.WriteLine($"File moved to: {destFile}");

                                processedFiles.Add(destFile);
                            }
                            catch (Exception ex)
                            {
                                recordlogdocument = new BsonDocument() { { "TypeWork", "Write" }, { "FileName", fileName },
                                { "TimeStartRecord", TimeStart }, { "TimeEndRecord", TimeEnd }, { "Error", ex.Message } };

                                _CDRsLogcollection.InsertOne(recordlogdocument);
                                Console.WriteLine($"Error moving file {fileName}: {ex.Message}");
                            }
                        }
                        catch (Exception ex)
                        {
                            var time = DateTime.Now;
                            string TimeStart = time.ToString("yyyy-MM-dd HH:mm:ss");
                            var fileName = Path.GetFileName(file);
                            var recordlogdocument = new BsonDocument() { { "TypeWork", "Write" }, { "FileName", fileName },
                                { "TimeStartRecord", TimeStart }, { "Status", "Error" }, { "Error", ex.Message } };

                            _CDRsLogcollection.InsertOne(recordlogdocument);

                            Console.WriteLine($"Error: {ex.Message}");
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
