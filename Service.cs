using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Broker
{
    public class Endpoints
    {
        public string fetch { get; set; }
        public string send { get; set; }
    }

    public class Api
    {
        public Endpoints endpoints { get; set; }
        public string host { get; set; }
        public string port { get; set; }
    }

    public class Database
    {
        public string host { get; set; }
        public string port { get; set; }
        public string username { get; set; }
        public string name { get; set; }
        public string password { get; set; }
    }

    public class Root
    {
        public bool enabled { get; set; }
        public string name { get; set; }
        public Api api { get; set; }
        public Database database { get; set; }
    }

    public class Program
    {

        static string connectionString;

        
        static void Main(string[] args)
        {

            

           
            try
            {
                ThreadClass();
                SqlDependency.Start(connectionString);
                RegisterSqlDependency();

            }
            catch (Exception exception)
            {
                ErrorLogging(exception);
                ReadError();
            }
            Console.WriteLine("Listening to database changes...");
            Console.ReadLine();
        }

        static void ThreadClass()
        {
            var responses = JsonConvert.DeserializeObject<List<Root>>(File.ReadAllText(@"settings.json"));


            var response = responses[0];

            connectionString = $"Persist Security Info=false;User ID={response.database.username} ; " +

                        $"password={response.database.password} ;Initial Catalog={response.database.name} ;" +

                        $" Data Source={response.database.host} ; Connection Timeout=100000;";

            Console.WriteLine(connectionString);

            
        }

        static void RegisterSqlDependency()
        {
            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                if (connection.State != System.Data.ConnectionState.Open)
                {
                    connection.Open();
                }


                string sqlQuery = "SELECT created_at, tableName, operation FROM dbo.Logs";

                SqlCommand command = new SqlCommand(sqlQuery, connection);

                SqlDependency dependency = new SqlDependency(command);

                dependency.OnChange += new OnChangeEventHandler(OnDependencyChange);

                using (SqlDataReader reader = command.ExecuteReader())
                {

                }
            }
        }

        static void OnDependencyChange(object sender, SqlNotificationEventArgs e)
        {
            var InsertOrUpdateOrDelte = e.Info;

            Console.WriteLine(InsertOrUpdateOrDelte);

            SqlDependency dependency = sender as SqlDependency;

            dependency.OnChange -= OnDependencyChange;

            RegisterSqlDependency();

        }
        public static void ErrorLogging(Exception ex)
        {
            string strPath = @"error.log";

            if (!File.Exists(strPath))
            {
                File.Create(strPath).Dispose();
            }
            using (StreamWriter sw = File.AppendText(strPath))
            {
                sw.WriteLine("=============Error Logging ===========");
                sw.WriteLine("===========Start============= " + DateTime.Now);
                sw.WriteLine("Error Message: " + ex.Message);
                sw.WriteLine("Stack Trace: " + ex.StackTrace);
                sw.WriteLine("===========End============= " + DateTime.Now);

            }
        }
        public static void ReadError()
        {
            string strPath = @"error.log";

            using (StreamReader sr = new StreamReader(strPath))
            {
                string line;
                while ((line = sr.ReadLine()) != null)
                {
                    Console.WriteLine(line);
                }
            }
        }
        public static void WriteConnectionStrings(string cncStr)
        {
            string strPath = @"strings.txt";

            if (!File.Exists(strPath))
            {
                File.Create(strPath).Dispose();
            }
            using (StreamWriter sw = File.AppendText(strPath))
            {
                sw.WriteLine(cncStr);
            }
        }
    }

}
