using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MediaButlerConsumer
{
    class Program
    {
        private static string ServiceBusName = "[Service Bus name as defined during configuration]";
        private static string ServiceBusSharedAccessKeyName = "[name of Shared Access Key]";
        private static string ServiceBusSharedAccessKey = "[Shared Access Key]";
        private static string ServiceBusConnectionString = "Endpoint=sb://{0}.servicebus.windows.net/;SharedAccessKeyName={1};SharedAccessKey={2}";
        private static string Topic = "Encoding";
        private static string SubscriptionName = "[Subscription Name as defined during configuration]";

        static void Main(string[] args)
        {
            // Construct the Service Bus connection tring
            ServiceBusConnectionString = string.Format(ServiceBusConnectionString, ServiceBusName, ServiceBusSharedAccessKeyName, ServiceBusSharedAccessKey);

            // Make sure everything is in place
            // CreateStuffIfNotExists();

            // Create a client that lisens to the Topic and Subscription
            var client = SubscriptionClient.CreateFromConnectionString(ServiceBusConnectionString, Topic, SubscriptionName);


            while (true)
            {
                Console.WriteLine("Waiting for messages");
                var message = client.Receive(TimeSpan.FromMinutes(1));
                if (message != null)
                {
                    try
                    {
                        // Process message from subscription
                        Console.WriteLine("\n**Encoding Message Received**");
                        Console.WriteLine("MessageID: " + message.MessageId);

                        // Deserialize the message body into an ExportDataAsset object
                        // Note that GetBody can only be called once per message, you wlil get an InvalidOperationException if you try otherwise
                        var messageBody = message.GetBody<string>();
                        var assetInfo = JsonConvert.DeserializeObject<ExportDataAsset>(messageBody);                            
                        if (assetInfo != null)
                        {
                            Console.WriteLine("Azure Media Service Asset Id: " + assetInfo.AssetId);
                            Console.WriteLine("Original File Name: " + assetInfo.AlternateId); // Will contain the whole link (ex: https://sabbourbutlermedia.blob.core.windows.net/testservicebus/Processing/horses.mp4)
                            Console.WriteLine("HLS: " + assetInfo.HLS);
                            Console.WriteLine("Smooth Streaming: " + assetInfo.Smooth);
                            Console.WriteLine("MPEG-DASH: " + assetInfo.DASH);
                        }

                        // Remove message from subscription (only after you complete any operations you need to do in your system)
                        message.Complete();
                        Console.WriteLine("Message removed from Service Bus");
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("Exception " + ex);
                        // Indicates a problem, unlock message in subscription
                        message.Abandon();
                    }
                }
            }
        }

        /// <summary>
        /// Make sure the topic and subscription are there. This needs Manage access on the policy
        /// </summary>
        private static void CreateStuffIfNotExists()
        {
            var namespaceManager = NamespaceManager.CreateFromConnectionString(ServiceBusConnectionString);

            // Create the topic if it does not exist already
            if (!namespaceManager.TopicExists(Topic))
            {
                namespaceManager.CreateTopic(Topic);
            }

            // Create the subscription if it does not exist already
            if (!namespaceManager.SubscriptionExists(Topic, SubscriptionName))
            {
                namespaceManager.CreateSubscription(Topic, SubscriptionName);
            }
        }
    }
}
