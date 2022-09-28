using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace PutBlockSample
{
    public class AppSettings
    {
        public string SourceAccount { get; set; }
        public string DestinationAccount { get; set; }
    }
}
