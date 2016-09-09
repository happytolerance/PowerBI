using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Insight.BackendJobs.Utilities;

namespace Insight.BackendJobs.GetOPSMetrics
{
    public abstract class AbstractETL : IETL
    {
        public static object SharedObject_Prod_GitHub;
        public static object SharedObject_Prod_VSO;
        public static object SharedObject_Prod_All;
        protected ConfigManager configManager = new ConfigManager();
        protected string OPSDataSyncConnStr = "";

        public void ETL()
        {
            OPSDataSyncConnStr = configManager.GetConfig("DBConnectionString", "OPSDataSync");
            object objE = Extract();
            object objT = Transform(objE);
            Load(objT);
        }

        protected abstract object Extract();

        protected abstract object Transform(object obj);

        protected abstract void Load(object obj);
    }
}
