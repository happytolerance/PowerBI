using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Insight.BackendJobs.InsightDBHelper;

namespace Insight.BackendJobs.GetOPSMetrics
{
    public class GitVSOPullETL : AbstractETL
    {
        protected override object Extract()
        {

            CombineList ret = new CombineList();

            //Mapping the GitVSORepositoryId with GitRepositoryId
            Dictionary<string, string> dic = new Dictionary<string, string>();
            string vsoAccountsString = configManager.GetConfig("BackendJobs", "VSOAccounts");
            string[] vsoAccounts = vsoAccountsString.Split(';');

            foreach (string account in vsoAccounts)
            {
                string vsRepoUrl = string.Format("https://{0}.visualstudio.com/DefaultCollection/_apis/git/repositories?api-version=1.0", account);
                GitVSORepositoryList vsRepoList = Util.CallGitVSOAPI<GitVSORepositoryList>(vsRepoUrl) as GitVSORepositoryList;
                foreach (GitVSORepository vsRepo in vsRepoList.Value as List<GitVSORepository>)
                {
                    //The group may have repos with same name, here the url is used rather than name to distinguish them. 
                    if (!dic.ContainsKey(vsRepo.RemoteUrl))
                        dic.Add(vsRepo.RemoteUrl, vsRepo.Id);
                }
            }


            List<GitVSOPull> vsNewPullList = new List<GitVSOPull>();
            List<GitVSOPull> vsUpdatePullList = new List<GitVSOPull>();
            List<GitVSOUser> vsUserList = new List<GitVSOUser>();

            List<GitHubRepository> repos = SharedObject_Prod_VSO as List<GitHubRepository>;
            Dictionary<string, string> vsUserDic = new Dictionary<string, string>();

            foreach (GitHubRepository repo in repos)
            {
                if (!dic.ContainsKey(repo.RepositoryUrl)) continue;
                string vsoRepoId = dic[repo.RepositoryUrl];

                //Select the lastest pullRequest number in database
                int recordedLatestPullNumber = -1;
                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                DataRow[] dataRow = InsightDBHelper.InsightDBHelper.ExecuteQuery(
                    string.Format("SELECT MAX(PullNumber) FROM OPS_VSOPulls WITH(NOLOCK) WHERE RepositoryId = '{0}'", repo.PartitionKey));
                if (dataRow != null && dataRow.Length > 0)
                {
                    if (!string.IsNullOrEmpty(dataRow[0].ItemArray[0].ToString()))
                        recordedLatestPullNumber = int.Parse(dataRow[0].ItemArray[0].ToString());
                }

                //Select the new pull requests not in database
                string[] statuses = { "active", "abandoned", "completed" };
                foreach (string status in statuses)
                {
                    int skipPageNum = 0, count = 0, minPullRequestNum = 0;
                    do
                    {
                        string pullRequestUrl = string.Format("https://{0}.visualstudio.com/_apis/git/repositories/{1}/pullRequests?api-version=1.0&status={2}&$skip={3}&$top=100",
                            repo.Owner, vsoRepoId, status, (skipPageNum++) * 100);
                        GitVSOPullList vsRullRequestList = Util.CallGitVSOAPI<GitVSOPullList>(pullRequestUrl) as GitVSOPullList;
                        count = vsRullRequestList.Count;
                        if (count == 0) break;

                        List<GitVSOPull> value = vsRullRequestList.Value;

                        //add new VSO users and new VSO Pulls
                        foreach (GitVSOPull vsPull in value)
                        {
                            vsPull.GitRepoId = repo.PartitionKey;
                            if (vsPull.pullRequestId > recordedLatestPullNumber)
                            {
                                vsNewPullList.Add(vsPull);
                                GitVSOUser vsUser = vsPull.CreatedBy;
                                if (!vsUserDic.ContainsKey(vsUser.ID))
                                {
                                    vsUserDic.Add(vsUser.ID, vsUser.DisplayName + "?" + vsUser.UniqueName);
                                    vsUserList.Add(vsUser);
                                }
                            }
                            else break;
                        }
                        //pullRequestId of last element in this page
                        minPullRequestNum = value[value.Count - 1].pullRequestId;
                    } while (count == 100 && minPullRequestNum > recordedLatestPullNumber);
                }

                //Select the pullRequest whose status is 'active' in database
                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                dataRow = InsightDBHelper.InsightDBHelper.ExecuteQuery(
                    string.Format("SELECT PullNumber FROM OPS_VSOPulls WITH(NOLOCK) WHERE RepositoryId = '{0}' AND Status = 'active' AND (TargetRefName LIKE '%master%' OR TargetRefName LIKE '%live%')", 
                        repo.PartitionKey), true);

                //Updated pull requests whose status changes from 'active' to 'completed' or 'abandoned'
                if (dataRow != null && dataRow.Length > 0)
                {
                    foreach (DataRow row in dataRow)
                    {
                        int pullRequestId = int.Parse(row.ItemArray[0].ToString());
                        string pullRequestUrlById = string.Format("https://{0}.visualstudio.com/_apis/git/repositories/{1}/pullRequests/{2}?api-version=1.0",
                            repo.Owner, vsoRepoId, pullRequestId);
                        GitVSOPull vsPullRequest = Util.CallGitVSOAPI<GitVSOPull>(pullRequestUrlById) as GitVSOPull;
                        if (!vsPullRequest.Status.Equals("active", StringComparison.OrdinalIgnoreCase))
                        {
                            vsPullRequest.GitRepoId = repo.PartitionKey;
                            vsUpdatePullList.Add(vsPullRequest);
                        }
                    }
                }
            }

            ret.vsNewPullList = vsNewPullList;
            ret.vsUpdatePullList = vsUpdatePullList;
            ret.vsUserList = vsUserList;

            return ret;
        }
                

        
        protected override object Transform(object obj)
        {
            return obj;
        }

        protected override void Load(object obj)
        {
            CombineList combine = obj as CombineList;
            List<GitVSOPull> vsNewPullList = combine.vsNewPullList;
            List<GitVSOPull> vsUpdatePullList = combine.vsUpdatePullList;
            List<GitVSOUser> vsUserList = combine.vsUserList;
           
            using (DataTable dt = new DataTable(), dt2 = new DataTable(), dt3 = new DataTable())
            {
                dt.Columns.Add("RepositoryId");
                dt.Columns.Add("PullNumber");
                dt.Columns.Add("TargetRefName");
                dt.Columns.Add("SourceRefName");
                dt.Columns.Add("CommitCount");
                dt.Columns.Add("UserId");
                dt.Columns.Add("Status");
                dt.Columns.Add("MergeStatus");
                dt.Columns.Add("CreatedAt");
                dt.Columns.Add("ClosedAt");

                dt2.Columns.Add("RepositoryId");
                dt2.Columns.Add("PullNumber");
                dt2.Columns.Add("TargetRefName");
                dt2.Columns.Add("SourceRefName");
                dt2.Columns.Add("CommitCount");
                dt2.Columns.Add("UserId");
                dt2.Columns.Add("Status");
                dt2.Columns.Add("MergeStatus");
                dt2.Columns.Add("CreatedAt");
                dt2.Columns.Add("ClosedAt");

                dt3.Columns.Add("ID");
                dt3.Columns.Add("DisplayName");
                dt3.Columns.Add("UniqueName");

                foreach (GitVSOPull vsPull in vsNewPullList)
                {
                    dt.Rows.Add(
                        vsPull.GitRepoId,
                        vsPull.pullRequestId,
                        vsPull.TargetRefName,
                        vsPull.SourceRefName,
                        vsPull.CommitsCount,
                        vsPull.CreatedBy.ID,
                        vsPull.Status,
                        vsPull.MergeStatus,
                        vsPull.CreationDate,
                        vsPull.ClosedDate
                        );
                }

                foreach (GitVSOPull vsPull in vsUpdatePullList)
                {
                    dt2.Rows.Add(
                        vsPull.GitRepoId,
                        vsPull.pullRequestId,
                        vsPull.TargetRefName,
                        vsPull.SourceRefName,
                        vsPull.CommitsCount,
                        vsPull.CreatedBy.ID,
                        vsPull.Status,
                        vsPull.MergeStatus,
                        vsPull.CreationDate,
                        vsPull.ClosedDate
                        );
                }

                foreach (GitVSOUser vsUser in vsUserList)
                {
                    dt3.Rows.Add(
                        vsUser.ID,
                        vsUser.DisplayName,
                        vsUser.UniqueName
                        );
                }

                Dictionary<string, DataTable> paramDic = new Dictionary<string, DataTable>();
                paramDic.Add("GitVSOPullTableParam?GitVSOPullType", dt);
                paramDic.Add("GitVSOPullTableParam2?GitVSOPullType", dt2);
                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitVSOPull", paramDic);

                Dictionary<string, DataTable> paramDic1 = new Dictionary<string, DataTable>();
                paramDic1.Add("GitVSOUserTableParam?GitVSOUserType", dt3);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitVSOUser", paramDic1, true);
            }
            return;
        }

        private class CombineList
        {
            public List<GitVSOPull> vsNewPullList;
            public List<GitVSOPull> vsUpdatePullList;
            public List<GitVSOUser> vsUserList;
        }
    }
}
