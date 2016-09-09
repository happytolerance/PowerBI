using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Net;
using System.Runtime.Serialization;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using Insight.BackendJobs.Utilities;

using InsightJobCommon;

namespace Insight.BackendJobs.GetOPSMetrics
{
    public class GetOPSMetrics : InsightJobBase
    {
        public override void RunJob(string parameters)
        {
            try
            {
                ETLFactory.Get("GitRepo").ETL();
                ETLFactory.Get("GitRepoDepotMapping").ETL();
                ETLFactory.Get("GitRepoTopicPublishHistory2").ETL();
                ETLFactory.Get("GitContributor").ETL();
                ETLFactory.Get("GitPull").ETL();
                ETLFactory.Get("GitVSOPull").ETL();
                ETLFactory.Get("GitRepoTopicLocalizationStatus").ETL();
                //ETLFactory.Get("GitRepoTopicCount").ETL();
                ETLFactory.Get("GitCommit").ETL();
                //ETLFactory.Get("GitRepoRecentUpdatedTopicCount").ETL();
                //ETLFactory.Get("GitRepoTopicPublishHistory").ETL();
                //ETLFactory.Get("OPSPublish").ETL();
            }
            catch (Exception ex)
            {
                this.TraceError(string.Format("GetOPSMetrics job failed: {0}", ex.ToString()));
                throw ex;
            }
        }
    }
}
