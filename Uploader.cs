using DBUpload.a_b_type;
using Newtonsoft.Json;
using Orient.Client;
using Orient.Client.API;
using Orient.Client.Mapping;
using Orient.Client.Protocol;
using OrientDB_Net.binary.Innov8tive.API;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBUpload
{
    class Uploader
    {
        public static string url = "tos_citation_network";
        public static int Upload(string url)
        {
            //connecting to the db
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, url, ODatabaseType.Graph, "admin", "admin");
            //creating auxiliary variables
            Query query = new Query();
            string json;
            string source;
            if (url[9] == 'a') source = "aminer";   //begins with a - aminer
            else source = "mag";                    //doesn't - mag
            //initializing string builder for batch
            string batch;
            StringBuilder builder = new StringBuilder("BEGIN;\n");
            //variables for output
            int counter = 0;
            //data objects for transaction
            List<string> aux_authors = new List<string>();
            List<string> aux_fos = new List<string>();
            List<string> aux_keyword = new List<string>();
            List<string> aux_references = new List<string>();
            //opening the file
            using (StreamReader sr = File.OpenText(url))
            {
                //processing each line
                while ((json = sr.ReadLine()) != null)
                {
                    //checking exception
                    try
                    {
                        //if upload failed at some point, then don't process uploaded lines
                        if (counter > 75280)
                        {
                            //deserialization
                            Vertex v = JsonConvert.DeserializeObject<Vertex>(json);
                            //parsing it:
                            //document_reference
                            Document_reference document_reference = new Document_reference();
                            document_reference.id = v.id;
                            document_reference.source = source;
                            document_reference.status = "0";
                            document_reference.component = 0;
                            //document_data
                            Document_data document_data = new Document_data();
                            document_data.doc_type = v.doc_type;
                            document_data.id = v.id;
                            document_data.pdf = v.pdf;
                            document_data.title = v.title;
                            document_data.url = v.url;
                            document_data.venue = v.venue;
                            document_data.year = v.year;
                            //checking if data already exists
                            Document_reference result = query.Select_id(document_reference.id);
                            //if exists -> update with data (was uploaded from reference, doesn't contain data)
                            if (result != null)
                            {
                                builder.Append("INSERT INTO document_data CONTENT ");
                                builder.Append(JsonConvert.SerializeObject(document_data));
                                builder.Append(";\n");
                            }
                            //if not -> create it with the data given
                            else
                            {
                                //creating command
                                //inserting document reference:
                                if (!aux_references.Contains(document_reference.id))
                                {
                                    aux_references.Add(document_reference.id);
                                    builder.Append("INSERT INTO document_reference CONTENT ");
                                    builder.Append(JsonConvert.SerializeObject(document_reference));
                                    builder.Append(";\n");
                                }
                                //inserting document data:
                                builder.Append("INSERT INTO document_data CONTENT ");
                                builder.Append(JsonConvert.SerializeObject(document_data));
                                builder.Append(";\n");
                            }
                            //uploading references
                            if (v.references != null) foreach (string refer in v.references)
                                {
                                    if (refer != null)
                                    {
                                        if (!aux_references.Contains(refer))
                                        {
                                            Document_reference refered_document = query.Select_id(refer);
                                            //if missing -> create it
                                            if (refered_document == null)
                                            {
                                                aux_references.Add(refer);
                                                //document_reference
                                                Document_reference document_reference_ref = new Document_reference();
                                                document_reference_ref.id = refer;
                                                document_reference_ref.source = source;
                                                document_reference_ref.status = "0";
                                                document_reference_ref.component = 0;
                                                //creating command
                                                builder.Append("INSERT INTO document_reference CONTENT ");
                                                builder.Append(JsonConvert.SerializeObject(document_reference_ref));
                                                builder.Append(";\n");
                                            }
                                        }
                                        //link it
                                        builder.Append(String.Format("CREATE EDGE reference FROM (SELECT FROM document_reference WHERE id = {0}) TO (SELECT FROM document_reference WHERE id = {1});\n"
                                                                    , query.Serializer(refer), query.Serializer(document_reference.id)));
                                    }
                                }
                            //processing keywords
                            if (v.keywords != null) foreach (string keyword in v.keywords)
                                {
                                    if (keyword != null)
                                    {
                                        //check if it's already part of the transaction
                                        if (!aux_keyword.Contains(keyword))
                                        {
                                            //if missing -> create it
                                            Keyword actual_keyword = query.Select_keyword_name(keyword);
                                            if (actual_keyword == null)
                                            {
                                                aux_keyword.Add(keyword);
                                                Keyword k = new Keyword();
                                                k.name = keyword;
                                                builder.Append("INSERT INTO keyword CONTENT {\"name\": ");
                                                builder.Append(JsonConvert.SerializeObject(keyword));
                                                builder.Append("};\n");
                                            }
                                        }
                                        //link it
                                        builder.Append(String.Format("CREATE EDGE using_keyword FROM (SELECT FROM keyword WHERE name = {0}) TO (SELECT FROM document_reference WHERE id = {1});\n"
                                                        , query.Serializer(keyword), query.Serializer(document_reference.id)));
                                    }
                                }
                            //processing fos
                            if (v.fos != null) foreach (string fos in v.fos)
                                {
                                    if (fos != null)
                                    {
                                        if (!aux_fos.Contains(fos))
                                        {
                                            //if missing -> create it
                                            Fos actual_fos = query.Select_fos_name(fos);
                                            if (actual_fos == null)
                                            {
                                                aux_fos.Add(fos);
                                                Fos f = new Fos();
                                                f.name = fos;
                                                builder.Append("INSERT INTO fos CONTENT {\"name\": ");
                                                builder.Append(JsonConvert.SerializeObject(f));
                                                builder.Append(";\n");
                                            }
                                            //link it
                                            builder.Append(String.Format("CREATE EDGE using_fos FROM (SELECT FROM fos WHERE name = {0}) TO (SELECT FROM document_reference WHERE id = {1});\n"
                                                            , query.Serializer(fos), query.Serializer(document_reference.id)));
                                        }
                                    }
                                }
                            //processing authors
                            if (v.authors != null) foreach (text_parse.Author author in v.authors)
                                {
                                    if (author.name != null)
                                    {
                                        if (!aux_authors.Contains(author.name))
                                        {
                                            //if missing -> create it
                                            a_b_type.Author actual_author = query.Select_author_name(author.name);
                                            if (actual_author == null)
                                            {
                                                aux_authors.Add(author.name);
                                                a_b_type.Author a = new a_b_type.Author();
                                                a.name = author.name;
                                                a.org = author.org;
                                                builder.Append("INSERT INTO author CONTENT ");
                                                builder.Append(JsonConvert.SerializeObject(a));
                                                builder.Append(";\n");
                                            }
                                            //link it
                                            builder.Append(String.Format("CREATE EDGE written_by_author FROM (SELECT FROM author WHERE name = {0}) TO (SELECT FROM document_reference WHERE id = {1});\n"
                                                          , query.Serializer(author.name), query.Serializer(document_reference.id)));
                                        }
                                    }
                                }
                            counter++;
                            //adding a commit if limit reached
                            if (counter % 10 == 0)
                            {
                                builder.Append("COMMIT RETRY 100;");
                                batch = builder.ToString();
                                System.IO.File.WriteAllText("log.txt", batch);
                                database.SqlBatch(batch).Run();
                                builder.Clear();
                                builder.Append("BEGIN;\n");
                                aux_authors.Clear();
                                aux_fos.Clear();
                                aux_keyword.Clear();
                                Console.Write("\r           " + url + " - " + DateTime.Now + " " + counter + " records finished");
                            }
                        }
                        //incrementing counter
                        else
                        {
                            counter++;
                            Console.Write("\r           " + url + " - " + DateTime.Now + " " + counter + " records finished");
                        }
                    }
                    catch (System.IO.IOException e)
                    {
                        System.Threading.Thread.Sleep(30000);
                    }
                }
            }
            database.Close();
            //returning how many records were uploaded
            return counter;
        }
        //calculates graph components
        public static int CalculateComponent(int current_component)
        {
            //connecting to the db
            OServer server = Server.Connect();
            ODatabase database = new ODatabase("localhost", 2424, url, ODatabaseType.Graph, "admin", "admin");
            //creating auxiliary variables
            Query query = new Query();
            ODocument starting_reference = new ODocument();
            int counter = 0;
            while ((starting_reference = query.SelectUnassignedComponent()) != null)
            {
                Console.WriteLine("Calculating component " + current_component);
                starting_reference.SetField("component", current_component);
                database.Transaction.Update(starting_reference);
                var results = query.TraverseComponent(starting_reference.GetField<string>("id"));
                if(results != null)
                    foreach (ODocument result in results)
                    {
                        result.SetField("component", current_component);
                        database.Transaction.Update(result);
                        Console.Write("\r           " + DateTime.Now + " - component " + current_component + " calculated for " + " " + ++counter + " document_reference");
                    }

                if (results != null)
                    database.Transaction.Commit();
                current_component++;
                counter = 0;
            }
            database.Close();
            return current_component;
        }
    }
}
