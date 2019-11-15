using MongoDB.Bson;
using MongoDB.Driver;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ReceiveRmqAndSaveMongoDb
{
    class Program
    {
        static void Main(string[] args)
        {
            ConnectionFactory factory = new ConnectionFactory();
            factory.UserName = "xx";
            factory.Password = "xx!";
            factory.VirtualHost = "/xx";
            factory.HostName = "localhost";
            
            using (var connection = factory.CreateConnection())
            using (var channel = connection.CreateModel())
            {
                
                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += (model, ea) =>
                {
                    var body = ea.Body;
                    var message = Encoding.UTF8.GetString(body);
                    Console.WriteLine(" [x] Received {0}", message);
                };
                channel.BasicConsume(queue: "atcs_video",
                                     autoAck: true,
                                     consumer: consumer);

                Console.WriteLine(" Press [enter] to exit.");
                Console.ReadLine();
            }

            dynamic msg = new JObject();
            msg.path = "path";
            msg.filename = "vide filename";


            dynamic stuff = JObject.Parse(msg.ToString());

            string path = stuff.path;
            string filename = stuff.filename;

            
            MainAsync(path,filename).Wait();

            Console.ReadLine();

        }

        static async Task MainAsync(String path , String filename)
        {

            var client = new MongoClient();

            IMongoDatabase db = client.GetDatabase("atcs_lampung");
            var collection = db.GetCollection<BsonDocument>("cctv");

            var document = new BsonDocument
                    {
                      {"path", new BsonString(path)},
                      {"filename", new BsonString(filename)},
                      
                    };
            await collection.InsertOneAsync(document);
        }
    }
}
