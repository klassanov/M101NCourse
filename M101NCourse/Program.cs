using MongoDB.Bson;
using MongoDB.Driver;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace M101NCourse
{
    class Program
    {
        static void Main(string[] args)
        {
            //MainAsynch(args).Wait();           
            //DoHomework21().Wait();
            //DoHomework22().Wait();

            DoHomework31().Wait();

            Console.WriteLine("Press enter");
            Console.ReadKey();
        }

        static IMongoCollection<BsonDocument> GetGradesCollection()
        {
            var connectionString = "mongodb://localhost:27017";
            var client = new MongoClient(connectionString);
            var db = client.GetDatabase("students");
            return db.GetCollection<BsonDocument>("grades");
        }

        static void DeleteGradeByStudentId(BsonValue student_id)
        {

        }

        static IMongoCollection<BsonDocument> GetCollection(string database, string collection)
        {
            var connectionString = "mongodb://localhost:27017";
            var client = new MongoClient(connectionString);
            var db = client.GetDatabase(database);
            return db.GetCollection<BsonDocument>(collection);
        }

        static async Task DoHomework31()
        {
            var builder = Builders<BsonDocument>.Filter.Exists("scores");
            var studentsDBCollection = GetCollection("school", "students");
            var studentsList = await studentsDBCollection.Find(builder).ToListAsync();

            foreach (var student in studentsList)
            {
                var scores = student["scores"].AsBsonArray;

                //int initialNumber=scores.Values.Count();

                var homeworks = scores.Where(x => x["type"].Equals("homework")).OrderBy(x => x["score"]);
                if (homeworks != null && homeworks.Count() > 0)
                {
                    var homeworkToRemove = homeworks.First();
                    bool removed = scores.Remove(homeworkToRemove);
                }
               

                //int finalNumber = scores.Values.Count();
                
                await studentsDBCollection.UpdateOneAsync(
                    new BsonDocument("_id", student["_id"]),
                    new BsonDocument("$set", new BsonDocument("scores", scores))
                    );

            }
        }


        static async Task DoHomework22()
        {
            string student_id = "student_id";
            var col = GetGradesCollection();
            var filter = new BsonDocument("type", "homework");
            var gradesList = await col.Find(filter)
               .Sort("{student_id:1, score:-1}")
               .ToListAsync();

            if (gradesList != null && gradesList.Count > 0)
            {
                int j = 0;
                for (int i = 0; i < gradesList.Count; i++)
                {
                    var currentStudent_Id = gradesList[i][student_id];
                    if (i + 1 < gradesList.Count)
                    {
                        var nextStudentId = gradesList[i + 1][student_id];
                        if (!currentStudent_Id.Equals(nextStudentId))
                        {
                            //await col.DeleteOneAsync(new BsonDocument("_id", gradesList[i]["_id"]));
                            await col.DeleteOneAsync(gradesList[i]);
                            //j++;
                        }
                    }
                }
                await col.DeleteOneAsync(gradesList.Last());
                Console.WriteLine("Deletions done");
            }
        }


        static async Task DoHomework21()
        {
            var connectionString = "mongodb://localhost:27017";
            var client = new MongoClient(connectionString);
            var db = client.GetDatabase("students");
            var col = db.GetCollection<BsonDocument>("grades");

            var filter = new BsonDocument("score", new BsonDocument("$gte", 65));

            var gradesList = await col.Find(filter)
                .Sort("{score:1}")
                .ToListAsync();

            Console.WriteLine(gradesList.First());

            //foreach (var item in gradesList)
            //{
            //    Console.WriteLine(item);
            //}

            Console.WriteLine(gradesList.Count);
            Console.WriteLine(await col.CountDocumentsAsync(new BsonDocument()));
        }

        static async Task MainAsynch(string[] args)
        {

            var connectionString = "mongodb://localhost:27017";
            //var settings = new MongoClientSettings
            //{

            //};
            var client = new MongoClient(connectionString);
            var db = client.GetDatabase("test");
            var col = db.GetCollection<BsonDocument>("people");


            var doc = new BsonDocument
            {
                {"name","Alex"}
            };
            doc.Add("age", 33);
            doc["profession"] = "programmer";

            await col.InsertOneAsync(doc);

        }
    }
}
