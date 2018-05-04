using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Orient.Client;
using System.IO;
using Newtonsoft.Json;
using Newtonsoft.Json.Schema;
using DBUpload;
using System.Threading;
using DBUpload.a_b_type;

namespace DBUpload
{
    class Query
    {
        //DB name
        public string dbName = "tos_citation_network";
        //Selects a random document_reference with no component assigned
        public ODocument SelectUnassignedComponent()
        {

            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, dbName, ODatabaseType.Graph, "admin", "admin");
            try
            {
                List<ODocument> results = database.Select().From("document_reference").Where("component").Equals<int>(0).Limit(1).ToList();
                database.Close();
                if (results.Count < 1) return null;
                else return results.ElementAt(0);
            }
            catch (OException e)
            {
                Console.WriteLine("Error has occured during UnassignedComponent selection from DB:");
                Console.WriteLine(e.Message);
                database.Close();
                return null;
            }
        }
        //Traverse the graph from the document_reference identified by the given id
        public List<ODocument> TraverseComponent(string id)
        {

            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, dbName, ODatabaseType.Graph, "admin", "admin");
            try
            {
                List<ODocument> results = database.Command(String.Format("traverse out('reference'), in('reference') from (select from document_reference where id = '{0}')", id)).ToList(); //database.Select().From("document_reference").Where("component").Equals<int>(0).ToList<Document_reference>();
                database.Close();
                if (results.Count < 1) return null;
                else return results;
            }
            catch (OException e)
            {
                Console.WriteLine("Error has occured during UnassignedComponent selection from DB:");
                Console.WriteLine(e.Message);
                database.Close();
                return null;
            }
        }
        //Returns the document_reference with the given id
        public Document_reference Select_id(string id)
        {

            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, dbName, ODatabaseType.Graph, "admin", "admin");
            try
            {
                var results = database.Select().From("document_reference").Where("id").Equals<string>(id).ToList<Document_reference>();
                database.Close();
                if (results.Count < 1) return null;
                else return results.ElementAt(0);
            }
            catch (OException e)
            {
                Console.WriteLine("Error has occured during id selection from DB:");
                Console.WriteLine(e.Message);
                database.Close();
                return null;
            }
        }
        //Returns the keyword with the given name
        public Keyword Select_keyword_name(string name)
        {

            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, dbName, ODatabaseType.Graph, "admin", "admin");
            try
            {
                var results = database.Select().From("keyword").Where("name").Equals<string>(name).ToList<Keyword>();
                database.Close();
                if (results.Count < 1) return null;
                else return results.ElementAt(0);
            }
            catch (OException e)
            {
                Console.WriteLine("Error has occured during keyword selection from DB:");
                Console.WriteLine(e.Message);
                database.Close();
                return null;
            }
        }
        //Returns the fos with the given name
        public Fos Select_fos_name(string name)
        {

            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, dbName, ODatabaseType.Graph, "admin", "admin");
            try
            {
                var results = database.Select().From("fos").Where("name").Equals<string>(name).ToList<Fos>();
                database.Close();
                if (results.Count < 1) return null;
                else return results.ElementAt(0);
            }
            catch (OException e)
            {
                Console.WriteLine("Error has occured during fos selection from DB:");
                Console.WriteLine(e.Message);
                database.Close();
                return null;
            }
        }
        //Returns the author with the given name and org
        public Author Select_author_name(string name)
        {

            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, dbName, ODatabaseType.Graph, "admin", "admin");
            try
            {
                var results = database.Select().From("author").Where("name").Equals<string>(name).ToList<Author>();
                database.Close();
                if (results.Count < 1) return null;
                else return results.ElementAt(0);
            }
            catch (OException e)
            {
                Console.WriteLine("Error has occured during author selection from DB:");
                Console.WriteLine(e.Message);
                database.Close();
                return null;
            }
        }
        //Returns the given citation
        public Citation Select_citation_text(string citation)
        {

            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, dbName, ODatabaseType.Graph, "admin", "admin");
            try
            {
                var results = database.Select().From("citation").Where("citation").Equals<string>(citation).ToList<Citation>();
                database.Close();
                if (results.Count < 1) return null;
                else return results.ElementAt(0);
            }
            catch (OException e)
            {
                Console.WriteLine("Error has occured during citation selection from DB:");
                Console.WriteLine(e.Message);
                database.Close();
                return null;
            }
        }
        //If the parameter is null, returns "null" in string, else returns the original string
        public string Serializer(string input)
        {
            if (input == null) return "null";
            else
            {
                input = input.Replace("\\\"", "\"").Replace("\"", "\\\"");
                return "\"" + input + "\"";
            }
        }
    }
}
