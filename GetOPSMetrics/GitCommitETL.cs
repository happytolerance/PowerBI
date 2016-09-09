using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Insight.BackendJobs.GetOPSMetrics
{
    public class GitCommitETL : AbstractETL
    {
        protected override object Extract()
        {
            Dictionary<string, GitCommitValueAgg> ret = new Dictionary<string, GitCommitValueAgg>();

            HashSet<string> OPGitRepoBranchWhitelist = new HashSet<string>(configManager.GetConfig("BackendJobs", "OPGitRepoBranchWhitelist").Split(';'));
            List<GitHubRepository> repos = SharedObject_Prod_GitHub as List<GitHubRepository>;
            int pageUpperLimit = 10; //for one time, the commits# upper limit is 100(per_page) * 10(page_limit) = 1000 commits
            string utcNow = DateTime.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
            foreach (GitHubRepository repo in repos)
            {
#if DEBUG
                //if (!string.Equals(repo.PartitionKey, "4692e255-d85f-488b-7eda-35c94087b072", StringComparison.OrdinalIgnoreCase))
                //{
                //    continue;
                //}
#endif

                List<GitBranch> branches =
                    Util.CallGitHubAPI<List<GitBranch>>(string.Format("https://api.github.com/repos/{0}/{1}/branches", repo.Owner, repo.RepositoryName), repo.AuthToken) as List<GitBranch>;

                if(branches == null)
                {
                    continue;
                }

                repo.Branches = branches;

                foreach (GitBranch gitBranch in branches)
                {
                    if (!OPGitRepoBranchWhitelist.Contains(gitBranch.Name.ToLowerInvariant()))
                    {
                        continue;
                    }

                    string branch = gitBranch.Name;

                    List<GitCommit> branchCommitList = new List<GitCommit>();
                    int commitPageNO = 1;
                    string url_part1 =
                        string.Format("https://api.github.com/repos/{0}/{1}/commits?sha={2}&per_page=100&until={3}", repo.Owner, repo.RepositoryName, branch, utcNow);
                    string url = url_part1;

                    InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                    DataRow[] dataRow = InsightDBHelper.InsightDBHelper.ExecuteQuery(
                        string.Format(
                            "select distinct max(LatestCommitDateTime) as LatestStoredCommitDateTime from OPS_CommitAggregation with (nolock) where RepositoryId = '{0}' and Branch = '{1}'",
                            repo.PartitionKey,
                            branch),
                        true
                        );
                    if (!(dataRow == null
                        || dataRow.Length == 0
                        || dataRow[0].ItemArray == null
                        || dataRow[0].ItemArray.Length == 0
                        || dataRow[0].ItemArray[0] == null
                        || string.IsNullOrEmpty(dataRow[0].ItemArray[0].ToString())
                        || string.Equals(dataRow[0].ItemArray[0].ToString(), "NULL", StringComparison.OrdinalIgnoreCase))
                        )
                    {
                        string since = DateTime.SpecifyKind(DateTime.Parse(dataRow[0].ItemArray[0].ToString()).AddSeconds(1), DateTimeKind.Utc).ToString("yyyy-MM-ddTHH:mm:ssZ");
                        url += "&since=" + since;
                    }

                    url += string.Format("&page={0}", commitPageNO++);

                    List<GitCommit> tmpCommitList = Util.CallGitHubAPI<List<GitCommit>>(url, repo.AuthToken) as List<GitCommit>;
                    branchCommitList.AddRange(tmpCommitList);

                    while (tmpCommitList.Count == 100 && commitPageNO <= pageUpperLimit)
                    {
                        url = url.Substring(0, url.Length - 1) + (commitPageNO++).ToString();
                        tmpCommitList = Util.CallGitHubAPI<List<GitCommit>>(url, repo.AuthToken) as List<GitCommit>;
                        branchCommitList.AddRange(tmpCommitList);
                    }

                    foreach (GitCommit commit in branchCommitList)
                    {
                        if (commit.Commit == null || commit.Commit.Committer == null || string.IsNullOrEmpty(commit.Commit.Committer.Date))
                        {
                            continue;
                        }

                        string commitDate = DateTime.Parse(commit.Commit.Committer.Date).ToUniversalTime().ToString("yyyy-MM-dd");
                        string latestCommitDateTime = DateTime.Parse(commit.Commit.Committer.Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss");
                        bool isMergedCommit = false;
                        if (commit.Commit != null && !string.IsNullOrEmpty(commit.Commit.Message) && commit.Commit.Message.StartsWith("Merge pull request #", StringComparison.OrdinalIgnoreCase))
                        {
                            isMergedCommit = true;
                        }
                        string key = repo.PartitionKey + "?" + branch + "?" + commitDate;
                        if (ret.ContainsKey(key))
                        {
                            ret[key].CommitCount++;
                            if (isMergedCommit)
                            {
                                ret[key].CommitMergedCount++;
                            }
                        }
                        else
                        {
                            GitCommitValueAgg GitCommitValueAgg = new GitCommitValueAgg()
                            {
                                CommitCount = 1,
                                CommitMergedCount = isMergedCommit ? 1 : 0,
                                LatestCommitDateTime = latestCommitDateTime
                            };

                            ret.Add(key, GitCommitValueAgg);
                        }
                    }
                }
            }

            return ret;
        }

        protected override object Transform(object obj)
        {
            return obj;
        }

        protected override void Load(object obj)
        {
            if (obj == null)
            {
                return;
            }

            Dictionary<string, GitCommitValueAgg> objDic = obj as Dictionary<string, GitCommitValueAgg>;

            using (DataTable dt = new DataTable())
            {
                dt.Columns.Add("RepositoryId");
                dt.Columns.Add("Branch");
                dt.Columns.Add("CommitDate");
                dt.Columns.Add("CommitCount");
                dt.Columns.Add("CommitMergedCount");
                dt.Columns.Add("LatestCommitDateTime");
                foreach (KeyValuePair<string, GitCommitValueAgg> pair in objDic)
                {
                    string[] keyInfo = pair.Key.Split('?');
                    dt.Rows.Add(
                        keyInfo[0],
                        keyInfo[1],
                        keyInfo[2],
                        pair.Value.CommitCount,
                        pair.Value.CommitMergedCount,
                        pair.Value.LatestCommitDateTime
                        );
                }

                Dictionary<string, DataTable> paramDic = new Dictionary<string, DataTable>();
                paramDic.Add("GitCommitAggregationTableParam?GitCommitAggregationType", dt);

                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitCommitAggregation", paramDic, true);
            }
        }
    }
}
