using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Sas;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace PutBlockSample
{
    public class Program
    {

        private const int KiB = 1024;
        private const int MiB = KiB * 1024;
        private const int GiB = MiB * 1024;
        private const long TiB = GiB * 1024L;

        private static async Task Main(string[] args)
        {
            int chunkNumber = 0;

            //by default the upload SDK uses 4 MiB chunks, I did some testing and see the best results with 8 MiB chunks for this task
            //do your own testing to see what works best for your use case
            long chunkSize = 4 * MiB * 2;

            //i have an 8 core machine, and found 32 threads tasks per core yielded the best performance
            //do your own testing to see what works best for your use case
            SemaphoreSlim slim = new SemaphoreSlim(Environment.ProcessorCount * 32);

            //lets do some simple timing of the process
            var stopwatch = new Stopwatch();
            stopwatch.Start();

            //get the secrets from config
            var config = new ConfigurationBuilder()
                                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                                .AddJsonFile("appsettings.json")
                                .AddUserSecrets<Program>()
                                .Build();
            var appSettings = new AppSettings();
            config.Bind("AppSettings", appSettings);

            //get a handle to the source & destination containers
            BlobContainerClient sourceContainer = new BlobContainerClient(appSettings.SourceAccount, "test");
            BlobContainerClient destContainer = new BlobContainerClient(appSettings.DestinationAccount, "test");

            //what file do we want to copy
            string blobName = "big.dat";

            // Create a BlobClient representing the source blob to copy, and get its properties
            var sourceBlob = sourceContainer.GetBlockBlobClient(blobName);
            var sourceBlobProperties = (await sourceBlob.GetPropertiesAsync()).Value;

            Console.WriteLine($"Source File Length: {sourceBlobProperties.ContentLength:N0}");

            // Create a SAS token that's valid for one hour.
            // we will use this to give permissions to azure to get the source file from the destiation storage cluster
            BlobSasBuilder sasBuilder = new BlobSasBuilder()
            {
                BlobContainerName = sourceBlob.BlobContainerName,
                BlobName = sourceBlob.Name,
                Resource = "b"
            };

            // I only need read access for a short period of time
            sasBuilder.ExpiresOn = DateTimeOffset.UtcNow.AddHours(1);
            sasBuilder.SetPermissions(BlobSasPermissions.Read);

            // get the full URI to the source file
            Uri sasUri = sourceBlob.GenerateSasUri(sasBuilder);

            // Get a BlobClient representing the destination blob with a unique name.
            var destBlob = destContainer.GetBlockBlobClient(Guid.NewGuid() + "-" + sourceBlob.Name);

            // Can I copy the file in one chunk or do I need to break it up?
            // file is too big for blob storage (we should never hit this since the source file is already in azure)
            if (sourceBlobProperties.ContentLength > (long)sourceBlob.BlockBlobMaxStageBlockBytes * (long)sourceBlob.BlockBlobMaxBlocks)
            {
                throw new ArgumentOutOfRangeException($"File Too Big {sourceBlobProperties.ContentLength:N0} > {sourceBlob.BlockBlobMaxStageBlockBytes * sourceBlob.BlockBlobMaxBlocks}");
            }
            // file is small enough to copy in one chunk
            else if (sourceBlobProperties.ContentLength < sourceBlob.BlockBlobMaxUploadBlobBytes)
            {
                await destBlob.SyncUploadFromUriAsync(sasUri);
            }
            // file is too big to copy in one chunk, need to break into blocks and upload individually
            else
            {
                // keep track of all the block names in order, we will need this to "put humpty dumpty back together again"
                var blockList = new List<string>();

                // keep track of all the .NET tasks that are created. 
                var taskList = new List<Task>();

                // keep track of where we are in the source block
                long currentByteIndex = 0L;

                // Calculate the block size, use the default provided if you can, or use something bigger to keep the number of blocks under the limit
                long blockSize = (long)Math.Max(chunkSize, Math.Round((sourceBlobProperties.ContentLength / (double)sourceBlob.BlockBlobMaxBlocks), MidpointRounding.ToPositiveInfinity));

                // read through the source blob till you get to the end. 
                while (currentByteIndex < sourceBlobProperties.ContentLength)
                {
                    // I typically want to read the blocksize, however I might need to read less if I am getting close to the end of the file
                    long readAmount = Math.Min(currentByteIndex + blockSize, sourceBlobProperties.ContentLength) - currentByteIndex;

                    // generate a unique name for the block
                    string blockID = Convert.ToBase64String(Guid.NewGuid().ToByteArray());

                    // add the name to the list
                    blockList.Add(blockID);

                    // create a DTO to hold the info about the block
                    var block = new Block(blockID, chunkNumber, currentByteIndex, currentByteIndex + readAmount);

                    // increment our counters
                    currentByteIndex += readAmount;
                    chunkNumber++;

                    // Create tasks and throw them into the list
                    taskList.Add(Task.Run(async () =>
                    {
                        // only allow the right number of tasks to run concurrently
                        slim.Wait();


                        Console.WriteLine($"Uploading Block {block.ChunkNumber} {block.BlockID} ({block.Start:N0} - {block.End:N0}) - {block.End - block.Start:N0}");

                        try
                        {
                            // tell the destination storage cluster to get the block from the source cluster
                            await destBlob.StageBlockFromUriAsync(sasUri, block.BlockID, new StageBlockFromUriOptions() { SourceRange = new Azure.HttpRange(block.Start, block.End - block.Start) });
                        }
                        catch (Exception ex)
                        {
                            //TODO: need retry logic if the above command fails
                            throw;
                        }

                        Console.WriteLine($"Uploading Block {block.ChunkNumber} {block.BlockID} ({block.Start:N0} - {block.End:N0}) - {block.End - block.Start:N0} complete");

                        // tell the Semaphore that I am done, and to allow the next one to start 
                        slim.Release();
                    }));


                }

                // Wait for all the tasks to run to completion
                Task.WaitAll(taskList.ToArray());

                // upload the block list
                await destBlob.CommitBlockListAsync(blockList);
            }

            Console.WriteLine($"Finished {stopwatch.Elapsed:ddd\\.hh\\:mm\\:ss} ({(sourceBlobProperties.ContentLength / stopwatch.Elapsed.TotalSeconds) * 8 / MiB:N} Mbps)");

        }

        

    }
}
