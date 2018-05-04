using DBUpload.text_parse;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBUpload
{

    class Vertex
    {
        public string id;
        public string title;
        public Author[] authors;
        public string venue;
        public int year;
        public string[] keywords;
        public string[] fos;
        public int n_citation;
        public string[] references;
        public string page_stat;
        public string page_end;
        public string doc_type;
        public string lang;
        public string publisher;
        public string volume;
        public string issue;
        public string issn;
        public string isbn;
        public string doi;
        public string pdf;
        public string[] url;
        public string Abstract;
    }
}
