using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Insight.BackendJobs.InsightDBHelper;

namespace Insight.BackendJobs.GetOPSMetrics
{
    public class GitPullETL : AbstractETL
    {
        protected override object Extract()
        {
            List<List<GitPull>> ret = new List<List<GitPull>>(2);

            List<GitPull> new_pulls = new List<GitPull>();
            List<GitPull> recorded_pulls = new List<GitPull>();

            List<GitHubRepository> repos = SharedObject_Prod_GitHub as List<GitHubRepository>;
            int pageUpperLimit = 10;

            foreach (GitHubRepository repo in repos)
            {
#if DEBUG
                //if (!string.Equals(repo.PartitionKey, "bab27324-4c59-a348-1473-9c6d587e4020", StringComparison.OrdinalIgnoreCase))
                //{
                //    continue;
                //}
#endif
                List<GitPull> pulls = new List<GitPull>();
                List<GitPull> recorded_pulls_inner = new List<GitPull>();

                Dictionary<int, int> dic_PrevCommitNOInPull = new Dictionary<int, int>();
                int recordedLatestPullNumber = -1;
                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                DataRow[] dataRow = InsightDBHelper.InsightDBHelper.ExecuteQuery(
                    string.Format("select PullNumber, CommitCount from OPS_Pulls with (nolock) where RepositoryId = '{0}'",
                    repo.PartitionKey)
                    );

                if (dataRow != null && dataRow.Length > 0)
                {
                    recordedLatestPullNumber = dataRow.Max(v => int.Parse(v.ItemArray[0].ToString()));
                    foreach (DataRow row in dataRow)
                    {
                        dic_PrevCommitNOInPull.Add(int.Parse(row.ItemArray[0].ToString()), int.Parse(row.ItemArray[1].ToString()));
                    }
                }

                int pageNO = 1;
                string url =
                    string.Format("https://api.github.com/repos/{0}/{1}/pulls?state=all&per_page=100&page={2}", repo.Owner, repo.RepositoryName, pageNO++);
                List<GitPull> tmpPulls =
                    Util.CallGitHubAPI<List<GitPull>>(url, repo.AuthToken) as List<GitPull>;
                if (tmpPulls == null) continue;
                
                pulls.AddRange(tmpPulls);

                while (tmpPulls.Count == 100 && pageNO <= pageUpperLimit && !tmpPulls.Exists(v => v.Number <= recordedLatestPullNumber))
                {
                    url = url.Substring(0, url.Length - 1) + (pageNO++).ToString();
                    tmpPulls = Util.CallGitHubAPI<List<GitPull>>(url, repo.AuthToken) as List<GitPull>;
                    pulls.AddRange(tmpPulls);
                }

                pulls.RemoveAll(v => v.Number <= recordedLatestPullNumber); //New
                pulls.Sort((x, y) => x.Number.CompareTo(y.Number));

                foreach (GitPull pull in pulls)
                {
                    pull.GitRepoId = repo.PartitionKey;
                    pull.CommitsCount = GetCommitNumber(pull.Number, dic_PrevCommitNOInPull, repo);
                }

                //Select the pullRequest whose status is 'open' in database
                dataRow = InsightDBHelper.InsightDBHelper.ExecuteQuery(
                    string.Format("SELECT PullNumber FROM OPS_Pulls WITH(NOLOCK) WHERE RepositoryId = '{0}' AND State = 'open'", repo.PartitionKey), true);

                //Updated pull requests whose status changes from 'open' to 'closed'
                if (dataRow != null && dataRow.Length > 0)
                {
                    foreach (DataRow row in dataRow)
                    {
                        int pullNumber = int.Parse(row.ItemArray[0].ToString());
                        string pullRequestUrlById = string.Format("https://api.github.com/repos/{0}/{1}/pulls/{2}", repo.Owner, repo.RepositoryName, pullNumber);
                        GitPull pullRequest = Util.CallGitHubAPI<GitPull>(pullRequestUrlById, repo.AuthToken) as GitPull;
                        if (pullRequest.State.Equals("closed", StringComparison.OrdinalIgnoreCase))
                        {
                            pullRequest.GitRepoId = repo.PartitionKey;
                            //Update the commits number
                            pullRequest.CommitsCount = GetCommitNumber(pullRequest.Number, dic_PrevCommitNOInPull, repo, true);
                            recorded_pulls_inner.Add(pullRequest);
                        }
                    }
                }

                new_pulls.AddRange(pulls);
                recorded_pulls.AddRange(recorded_pulls_inner);
            }
            ret.Add(new_pulls);
            ret.Add(recorded_pulls);

            return ret;
        }

        protected override object Transform(object obj)
        {
            return obj;
        }

        protected override void Load(object obj)
        {
            List<List<GitPull>> list = obj as List<List<GitPull>>;
            List<GitPull> pulls = list[0];
            List<GitPull> legacy_pulls = list[1];

            using (DataTable dt = new DataTable(), dt2 = new DataTable())
            {
                dt.Columns.Add("RepositoryId");
                dt.Columns.Add("PullNumber");
                dt.Columns.Add("TargetBranch");
                dt.Columns.Add("SourceBranch");
                dt.Columns.Add("CommitCount");
                dt.Columns.Add("UserId");
                dt.Columns.Add("State");
                dt.Columns.Add("CreatedAt");
                dt.Columns.Add("UpdatedAt");
                dt.Columns.Add("ClosedAt");
                dt.Columns.Add("MergedAt");

                dt2.Columns.Add("RepositoryId");
                dt2.Columns.Add("PullNumber");
                dt2.Columns.Add("TargetBranch");
                dt2.Columns.Add("SourceBranch");
                dt2.Columns.Add("CommitCount");
                dt2.Columns.Add("UserId");
                dt2.Columns.Add("State");
                dt2.Columns.Add("CreatedAt");
                dt2.Columns.Add("UpdatedAt");
                dt2.Columns.Add("ClosedAt");
                dt2.Columns.Add("MergedAt");

                foreach (GitPull pull in pulls)
                {
                    dt.Rows.Add(
                        pull.GitRepoId,
                        pull.Number,
                        pull.Base.Ref,
                        pull.Head.Ref,
                        pull.CommitsCount,
                        pull.User.Id,
                        pull.State,
                        pull.Created_At,
                        pull.Updated_At,
                        pull.Closed_At,
                        pull.Merged_At
                        );
                }

                foreach (GitPull pull in legacy_pulls)
                {
                    dt2.Rows.Add(
                        pull.GitRepoId,
                        pull.Number,
                        pull.Base.Ref,
                        pull.Head.Ref,
                        pull.CommitsCount,
                        pull.User.Id,
                        pull.State,
                        pull.Created_At,
                        pull.Updated_At,
                        pull.Closed_At,
                        pull.Merged_At
                        );
                }

                Dictionary<string, DataTable> paramDic = new Dictionary<string, DataTable>();
                paramDic.Add("GitPullTableParam?GitPullType", dt);
                paramDic.Add("GitPullTableParam2?GitPullType", dt2);

                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitPull", paramDic, true);
            }

            return;
        }

        private int GetCommitNumber(int pullNumber, Dictionary<int, int> dic_PrevCommitNOInPull, GitHubRepository repo, bool isUpdate = false)
        {
            if (!isUpdate)
            {
                if (dic_PrevCommitNOInPull.ContainsKey(pullNumber)) return dic_PrevCommitNOInPull[pullNumber];
            }
            
            string commitNOUrl = string.Format("https://api.github.com/repos/{0}/{1}/pulls/{2}", repo.Owner, repo.RepositoryName, pullNumber);
            GitCommitNOInPull gitCommitNOInPull = Util.CallGitHubAPI<GitCommitNOInPull>(commitNOUrl, repo.AuthToken) as GitCommitNOInPull;
            int commitsNOInPull = gitCommitNOInPull == null ? 0 : gitCommitNOInPull.Commits;

            if (commitsNOInPull == 0) return 0;

            string commitInPullUrl = string.Format("https://api.github.com/repos/{0}/{1}/pulls/{2}/commits", repo.Owner, repo.RepositoryName, pullNumber);
            List<GitCommit> commitsInPull = Util.CallGitHubAPI<List<GitCommit>>(commitInPullUrl, repo.AuthToken) as List<GitCommit>;

            foreach (GitCommit commit in commitsInPull)
            {
                GitInnerCommit innerCommit = commit.Commit;
                if (innerCommit == null || innerCommit.Committer == null || string.IsNullOrEmpty(innerCommit.Committer.Date)) continue;

                //This commit is another PullRequest
                if (innerCommit != null && !string.IsNullOrEmpty(innerCommit.Message) && innerCommit.Message.StartsWith("Merge pull request #", StringComparison.OrdinalIgnoreCase))
                {
                    int mergedPullNO = -1, prevCommitNO;
                    string message = innerCommit.Message;
                    string[] tmp = message.Split(' ');
                    int.TryParse(tmp[3].Split(new char[] { '#' }, StringSplitOptions.RemoveEmptyEntries)[0], out mergedPullNO);

                    if (dic_PrevCommitNOInPull.ContainsKey(mergedPullNO)) prevCommitNO = dic_PrevCommitNOInPull[mergedPullNO]; //Already exists
                    else
                    {
                        prevCommitNO = GetCommitNumber(mergedPullNO, dic_PrevCommitNOInPull, repo);
                    }
                    commitsNOInPull = commitsNOInPull - 1 + prevCommitNO;
                }
            }
            if (isUpdate) dic_PrevCommitNOInPull.Remove(pullNumber);
            dic_PrevCommitNOInPull.Add(pullNumber, commitsNOInPull); //Add new item

            return commitsNOInPull;
        }
    }
}
