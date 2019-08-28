//great code percentage taken from https://github.com/Azure/azure-cosmosdb-bulkexecutor-dotnet-getting-started﻿


using System;
using System.Threading.Tasks;
using System.Linq;
using System.Configuration;
using System.Collections.Generic;
using System.Net;
using Microsoft.Azure.CosmosDB;
using MoreLinq;
using System.Data.SqlClient;
using System.IO;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.CosmosDB.BulkExecutor;
using Microsoft.Azure.Documents;
using CosmosApp;
using System.Diagnostics;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.CosmosDB.BulkExecutor.BulkImport;
using System.Threading;
using ConnectionMode = Microsoft.Azure.Documents.Client.ConnectionMode;

namespace CosmosGettingStarted
{
    class Program
    {

        #region config cosmosdb connection 



        private DocumentClient client;
        /// <summary>
        /// Initializes a new instance of the <see cref="Program"/> class.
        /// </summary>
        /// <param name="client">The DocumentDB client instance.</param>
        private Program(DocumentClient client)
        {
            this.client = client;
        }


        private static readonly ConnectionPolicy ConnectionPolicy = new ConnectionPolicy
        {
            ConnectionMode = ConnectionMode.Direct,
            ConnectionProtocol = Protocol.Tcp
        };


        private static readonly string EndpointUrl = ConfigurationManager.AppSettings["EndPointUrl"];
        private static readonly string AuthorizationKey = ConfigurationManager.AppSettings["AuthorizationKey"];
        private static readonly string DatabaseName = ConfigurationManager.AppSettings["DatabaseName"];
        private static readonly string CollectionName = ConfigurationManager.AppSettings["CollectionName"];
        private static readonly int CollectionThroughput = int.Parse(ConfigurationManager.AppSettings["CollectionThroughput"]);

        #endregion

        public static async Task Main(string[] args)
        {
           
            try
            {
                using (var client = new DocumentClient(
                    new Uri(EndpointUrl),
                    AuthorizationKey,
                    ConnectionPolicy))
                {
                    var program = new Program(client);
                    program.RunBulkImportAsync().Wait();

                    await program.GPSAnalysis();
                }
            }
            catch (AggregateException e)
            {
                Trace.TraceError("Caught AggregateException in Main, Inner Exception:\n" + e);
                Console.ReadKey();
            }
            finally
            {
                Console.WriteLine("This is the end, press any key to exit.");
                Console.ReadKey();
            }
        }


        /// <summary>
        /// Driver function for bulk import.
        /// </summary>
        /// <returns></returns>
        private async Task RunBulkImportAsync()
        {
            // Cleanup on start if set in config.

            DocumentCollection dataCollection = null;
            try
            {
                if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnStart"]))
                {
                    Microsoft.Azure.Documents.Database database = Utils.GetDatabaseIfExists(client, DatabaseName);
                    if (database != null)
                    {
                        await client.DeleteDatabaseAsync(database.SelfLink);
                    }

                    Trace.TraceInformation("Creating database {0}", DatabaseName);
                    database = await client.CreateDatabaseAsync(new Microsoft.Azure.Documents.Database { Id = DatabaseName });

                    Trace.TraceInformation(String.Format("Creating collection {0} with {1} RU/s", CollectionName, CollectionThroughput));
                    dataCollection = await Utils.CreatePartitionedCollectionAsync(client, DatabaseName, CollectionName, CollectionThroughput);
                }
                else
                {
                    dataCollection = Utils.GetCollectionIfExists(client, DatabaseName, CollectionName);
                    if (dataCollection == null)
                    {
                        throw new Exception("The data collection does not exist");
                    }
                }
            }
            catch (Exception de)
            {
                Trace.TraceError("Unable to initialize, exception message: {0}", de.Message);
                throw;
            }

            // Prepare for bulk import.

            // Creating documents with simple partition key here.
            string partitionKeyProperty = dataCollection.PartitionKey.Paths[0].Replace("/", "");

            long numberOfDocumentsToGenerate = long.Parse(ConfigurationManager.AppSettings["NumberOfDocumentsToImport"]);
            int numberOfBatches = int.Parse(ConfigurationManager.AppSettings["NumberOfBatches"]);
            long numberOfDocumentsPerBatch = (long)Math.Floor(((double)numberOfDocumentsToGenerate) / numberOfBatches);

            // Set retry options high for initialization (default values).
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 30;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 9;

            IBulkExecutor bulkExecutor = new BulkExecutor(client, dataCollection);
            await bulkExecutor.InitializeAsync();

            // Set retries to 0 to pass control to bulk executor.
            client.ConnectionPolicy.RetryOptions.MaxRetryWaitTimeInSeconds = 0;
            client.ConnectionPolicy.RetryOptions.MaxRetryAttemptsOnThrottledRequests = 0;

            BulkImportResponse bulkImportResponse = null;
            long totalNumberOfDocumentsInserted = 0;
            double totalRequestUnitsConsumed = 0;
            double totalTimeTakenSec = 0;

            var tokenSource = new CancellationTokenSource();
            var token = tokenSource.Token;

            for (int i = 0; i < numberOfBatches; i++)
            {
                // Generate JSON-serialized documents to import.

                List<string> documentsToImportInBatch = new List<string>();
                long prefix = i * numberOfDocumentsPerBatch;

                Trace.TraceInformation(String.Format("Generating {0} documents to import for batch {1}", numberOfDocumentsPerBatch, i));
                for (int j = 0; j < numberOfDocumentsPerBatch; j++)
                {
                    string partitionKeyValue = (prefix + j).ToString();
                    string id = partitionKeyValue + Guid.NewGuid().ToString();

             
                }

                // Invoke bulk import API.

                var tasks = new List<Task>();

                tasks.Add(Task.Run(async () =>
                {
                    Trace.TraceInformation(String.Format("Executing bulk import for batch {0}", i));
                    do
                    {
                        try
                        {
                            bulkImportResponse = await bulkExecutor.BulkImportAsync(
                                documents: documentsToImportInBatch,
                                enableUpsert: true,
                                disableAutomaticIdGeneration: true,
                                maxConcurrencyPerPartitionKeyRange: null,
                                maxInMemorySortingBatchSize: null,
                                cancellationToken: token);
                        }
                        catch (DocumentClientException de)
                        {
                            Trace.TraceError("Document client exception: {0}", de);
                            break;
                        }
                        catch (Exception e)
                        {
                            Trace.TraceError("Exception: {0}", e);
                            break;
                        }
                    } while (bulkImportResponse.NumberOfDocumentsImported < documentsToImportInBatch.Count);

                    Trace.WriteLine(String.Format("\nSummary for batch {0}:", i));
                    Trace.WriteLine("--------------------------------------------------------------------- ");
                    Trace.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
                        bulkImportResponse.NumberOfDocumentsImported,
                        Math.Round(bulkImportResponse.NumberOfDocumentsImported / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                        Math.Round(bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.TotalTimeTaken.TotalSeconds),
                        bulkImportResponse.TotalTimeTaken.TotalSeconds));
                    Trace.WriteLine(String.Format("Average RU consumption per document: {0}",
                        (bulkImportResponse.TotalRequestUnitsConsumed / bulkImportResponse.NumberOfDocumentsImported)));
                    Trace.WriteLine("---------------------------------------------------------------------\n ");

                    totalNumberOfDocumentsInserted += bulkImportResponse.NumberOfDocumentsImported;
                    totalRequestUnitsConsumed += bulkImportResponse.TotalRequestUnitsConsumed;
                    totalTimeTakenSec += bulkImportResponse.TotalTimeTaken.TotalSeconds;
                },
                token));

                /*
                tasks.Add(Task.Run(() =>
                {
                    char ch = Console.ReadKey(true).KeyChar;
                    if (ch == 'c' || ch == 'C')
                    {
                        tokenSource.Cancel();
                        Trace.WriteLine("\nTask cancellation requested.");
                    }
                }));
                */

                await Task.WhenAll(tasks);
            }

            Trace.WriteLine("Overall summary:");
            Trace.WriteLine("--------------------------------------------------------------------- ");
            Trace.WriteLine(String.Format("Inserted {0} docs @ {1} writes/s, {2} RU/s in {3} sec",
                totalNumberOfDocumentsInserted,
                Math.Round(totalNumberOfDocumentsInserted / totalTimeTakenSec),
                Math.Round(totalRequestUnitsConsumed / totalTimeTakenSec),
                totalTimeTakenSec));
            Trace.WriteLine(String.Format("Average RU consumption per document: {0}",
                (totalRequestUnitsConsumed / totalNumberOfDocumentsInserted)));
            Trace.WriteLine("--------------------------------------------------------------------- ");

            // Cleanup on finish if set in config.

            if (bool.Parse(ConfigurationManager.AppSettings["ShouldCleanupOnFinish"]))
            {
                Trace.TraceInformation("Deleting Database {0}", DatabaseName);
                await client.DeleteDatabaseAsync(UriFactory.CreateDatabaseUri(DatabaseName));
            }

            Trace.WriteLine("\nPress any key to exit.");
            Console.ReadKey();
        }


        private async Task GPSAnalysis()
        {
            #region Workaround to be able to test the algorithms
            const string CONNECTION_STRING = @"Data Source=.;" +
                                 "Initial Catalog=test_cosmos;" +
                                 "Integrated Security=SSPI;";

            SqlConnection conn = new SqlConnection() { ConnectionString = CONNECTION_STRING  };
            List<String> ResultSet = new List<String>();
            SqlCommand cmd = new SqlCommand("SELECT * FROM operations", conn);

            conn.Open();
            using (SqlDataReader reader = cmd.ExecuteReader())
            {
                if (!reader.HasRows) return;

                try
                {
                    while (reader.Read())
                    {
                        var rec = reader.GetValue(0).ToString();
                        ResultSet.Add(rec);

                    }
                }
                catch (Exception e)
                {
                    // TODO: deal with exception e
                }

                reader.Close();

            }
            conn.Close();

            #endregion


            if (ResultSet != null && ResultSet.Count > 0)
            {

                Position origin = new Position(0, 0);
                List<Position> listPositions = new List<Position>() { origin };
                List<int> listDistance = new List<int>() { 0 };

                List<Marker> markers = new List<Marker>();

                int alpha = 0;
                int lastAlpha = 0;
                var distanceTravelled = 0;

                foreach (var instr in ResultSet)
                {
                    if (instr.ToLower().Contains("forward"))
                    {
                        int instruction = Int32.Parse(instr.Split(' ')[1].ToString());
                        Position.SetDistance(listDistance, instruction);
                        Position.SetPosition(listPositions, instruction, alpha, lastAlpha);
                        lastAlpha = alpha;
                        distanceTravelled += instruction;

                    }
                    else if (instr.ToLower().Contains("right"))
                    {
                        alpha = -1 * (Int32.Parse(instr.Split(' ')[1].ToString()));
                    }
                    else if (instr.ToLower().Contains("left"))
                    {
                        alpha = Int32.Parse(instr.Split(' ')[1].ToString());
                    }
                    else if (instr.ToLower().Contains("marker") || instr.ToLower().Contains("origin"))
                    {
                        //   GetMarkerPosition();
                        markers.Add(new Marker(instr, listPositions == null || listPositions.Count < 1 ? new Position(0, 0) : listPositions.Last(), distanceTravelled));
                        distanceTravelled = 0;
                    }

                }

                Console.WriteLine("Distance of the journey: ");
                Console.WriteLine(markers.Select(a => a.DistanceTravelledUntilMarker).ToList().Sum());
                Console.WriteLine("---------------------");
                Console.WriteLine("Distance from A to C: ");

                string start = "marker A";
                string end   = "marker C";
                Console.WriteLine(Position.MinimumDistance(listPositions,
                    markers.Where(x => x.Name.Equals(start)).Select(a => a.MarkerPosition).First(),
                    markers.Where(x => x.Name.Equals(end)).Select(a => a.MarkerPosition).First()));
                Console.WriteLine("---------------------");
                Console.WriteLine("Distance Travelled between A and C:");

                List<Marker> markersNeeded = new List<Marker>();
                markersNeeded = markers.SkipWhile(x => !x.Name.Equals(start)).TakeUntil(x => x.Name.Equals(end)).ToList().Where(x => !x.Name.Equals(start)).ToList();
                double totalDistance = markersNeeded.Select(a => a.DistanceTravelledUntilMarker).Sum();
                Console.WriteLine(totalDistance);


                Console.WriteLine("------------");
                Console.Read();

                var tcs = new TaskCompletionSource<int>();
                tcs.SetException(new NotImplementedException());
                
            }
        }
    }
}
