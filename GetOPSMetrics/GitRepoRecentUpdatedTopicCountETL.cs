using Octokit;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Insight.BackendJobs.GetOPSMetrics
{
    class GitRepoRecentUpdatedTopicCountETL : AbstractETL
    {

        protected override object Extract()
        {
            if (SharedObject_Prod_GitHub == null)
            {
                return null;
            }

            List<GitRepoRecentUpdatedTopicCountInfo> ret = new List<GitRepoRecentUpdatedTopicCountInfo>();
            List<GitHubRepository> repos = SharedObject_Prod_GitHub as List<GitHubRepository>;

            int day = int.Parse(configManager.GetConfig("BackendJobs", "OPGitRepoContentFreshDay"));

            foreach (var repo in repos)
            {
                if(repo.Branches == null || repo.Branches.Count == 0)
                {
                    continue;
                }

                Task<int> task = GetRecentUpdatedTopicCountOnLiveBranch(repo, day);
                task.Wait();
                ret.Add(new GitRepoRecentUpdatedTopicCountInfo()
                {
                    PartitionKey = repo.PartitionKey,
                    RecentUpdatedTopicCount = task.Result
                });
            }

            return ret;
        }

        protected override object Transform(object obj)
        {
            return obj;
        }

        protected override void Load(object obj)
        {
            List<GitRepoRecentUpdatedTopicCountInfo> infos = obj as List<GitRepoRecentUpdatedTopicCountInfo>;

            using (DataTable dt = new DataTable())
            {
                dt.Columns.Add("RepositoryId");
                dt.Columns.Add("RecentUpdatedTopicCount");

                foreach (var info in infos)
                {
                    dt.Rows.Add(info.PartitionKey, info.RecentUpdatedTopicCount);
                }

                Dictionary<string, DataTable> paramDic = new Dictionary<string, DataTable>();
                paramDic.Add("GitRepoRecentUpdatedTopicCountTableParam?GitRepoRecentUpdatedTopicCountType", dt);

                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitRepoRecentUpdatedTopicCount", paramDic, true);
            }
        }

        async Task<int> GetRecentUpdatedTopicCountOnLiveBranch(GitHubRepository repo, int day)
        {
            var github = new GitHubClient(new ProductHeaderValue("OPSMetrics"));
            var token = new Credentials(repo.AuthToken);
            github.Credentials = token;

            var repository = await github.Repository.Get(repo.Owner, repo.RepositoryName);

            var pullRequestRequest = new PullRequestRequest() { State = ItemState.Closed, Base = "live" };
            var pullRequests = await github.Repository.PullRequest.GetAllForRepository(repository.Owner.Login, repository.Name, pullRequestRequest);

            HashSet<string> recentUpdatedTopicList = new HashSet<string>();
            var timeThreshold = new DateTimeOffset(DateTime.UtcNow.AddDays(0 - day));
            foreach (var pullRequest in pullRequests)
            {
                if (pullRequest.MergedAt.HasValue && DateTimeOffset.Compare(timeThreshold, pullRequest.MergedAt.Value) <= 0)
                {
                    var merged = await github.PullRequest.Merged(repository.Owner.Login, repository.Name, pullRequest.Number);
                    if (merged)
                    {
                        var updatedFiles = await github.Repository.PullRequest.Files(repository.Owner.Login, repository.Name, pullRequest.Number);
                        foreach (var updatedFile in updatedFiles)
                        {
                            if (!(string.Equals(updatedFile.Status, "removed"))
                                && string.Equals(System.IO.Path.GetExtension(updatedFile.FileName), ".md"))
                            {
                                recentUpdatedTopicList.Add(updatedFile.FileName);
                            }
                        }
                    }
                }
            }

            return recentUpdatedTopicList.Count;
        }

    }
}
