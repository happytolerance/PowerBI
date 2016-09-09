using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Insight.BackendJobs.GetOPSMetrics
{
    public static class ETLFactory
    {
        public static IETL Get(string type)
        {
            IETL ret = null;
            switch (type)
            {
                case "GitRepo":
                    ret = new GitRepoETL();
                    break;
                case "GitRepoDepotMapping":
                    ret = new GitRepoDepotMappingETL();
                    break;
                case "GitContributor":
                    ret = new GitContributorETL();
                    break;
                case "GitCommit":
                    ret = new GitCommitETL();
                    break;
                case "GitPull":
                    ret = new GitPullETL();
                    break;
                case "GitRepoTopicCount":
                    ret = new GitRepoTopicCountETL();
                    break;
                case "GitRepoRecentUpdatedTopicCount":
                    ret = new GitRepoRecentUpdatedTopicCountETL();
                    break;
                case "OPSPublish":
                    ret = new OPSPublishETL();
                    break;
                case "GitRepoTopicLocalizationStatus":
                    ret = new GetTopicLocalizationStatusETL();
                    break;
                case "GitRepoTopicPublishHistory":
                    ret = new GitRepoTopicPublishHistoryETL();
                    break;
                case "GitRepoTopicPublishHistory2":
                    ret = new GitRepoTopicPublishHistory2ETL();
                    break;
                case "GitVSOPull":
                    ret = new GitVSOPullETL();
                    break;
                default:
                    break;
            }

            return ret;
        }
    }
}
