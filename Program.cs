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

namespace DBUpload
{
    class Program
    {
        //default settings
        static int DEFAULT_aminer_from  = 0;
        static int DEFAULT_aminer_to    = 1;//155;

        static int DEFAULT_mag_from     = 0;
        static int DEFAULT_mag_to       = 167;
        //settings
        static int aminer_from  = DEFAULT_aminer_from;
        static int aminer_to    = DEFAULT_aminer_to;
        static int mag_from     = DEFAULT_mag_from;
        static int mag_to       = DEFAULT_mag_to;
        //other variables
        static string url;
        static void DoUpload()
        {
            //uploading aminer
            //url = @"F:\graph\aminer_papers_";
            //for(int aminer_file = aminer_from; aminer_file < aminer_to; aminer_file++)
            //    Console.WriteLine(" ------ all" + Uploader.Upload(url + aminer_file + ".txt") + " records uploaded from aminer-" + aminer_file);
            //uploading mag
            //url = @"F:\graph\mag_papers_";
            //for (int mag_file = mag_from; mag_file < mag_to; mag_file++)
            //    Console.WriteLine(Uploader.Upload(url + mag_file + ".txt") + "records uploaded from mag-" + mag_file);
            Console.WriteLine("Finished uploading data!");
            //calculating components
            Console.WriteLine(Uploader.CalculateComponent(1) + " components succesfuly calculated");
            Console.ReadKey();
        }
        static void Main(string[] args)
        {
            DoUpload();
        }
    }
}