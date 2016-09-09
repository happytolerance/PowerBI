using Octokit;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Insight.BackendJobs.GetOPSMetrics
{
    class GitRepoTopicCountETL : AbstractETL
    {
        protected override object Extract()
        {
            if (SharedObject_Prod_GitHub == null)
            {
                return null;
            }

            List<GitRepoTopicInfo_Detail> ret = new List<GitRepoTopicInfo_Detail>();
            List<GitHubRepository> repos = SharedObject_Prod_GitHub as List<GitHubRepository>;
            foreach (var repo in repos)
            {
                if (repo == null)
                {
                    continue;
                }

                /*
                foreach (var branch in repo.Branches)
                {
                    Task<int> task = GetFileCountForExtension(repo, branch, "md");
                    task.Wait();
                    ret.Add(new GitRepoTopicInfo()
                    {
                        PartitionKey = repo.PartitionKey,
                        BranchName = branch.Name,
                        TopicCount = task.Result
                    });
                }
                 * */

                // Only for "live" branch that is the most important, and avoid the duplicate-db-key issue from possible same branch names
                try
                {
                    Task<List<string>> task = GetFileCountForExtension(repo, "live", "md");
                    task.Wait();
                    ret.Add(new GitRepoTopicInfo_Detail()
                    {
                        PartitionKey = repo.PartitionKey,
                        BranchName = "live",
                        Topics = task.Result
                    });
                }
                catch (System.AggregateException ex)
                {
                    if (ex.Message.Contains("Not Found"))
                    {
                        // ignore;
                    }
                    else
                    {
                        throw ex;
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
            List<GitRepoTopicInfo_Detail> infos = obj as List<GitRepoTopicInfo_Detail>;

            using (DataTable dt_TopicCount = new DataTable(), dt_TopicDetail = new DataTable())
            {
                dt_TopicCount.Columns.Add("RepositoryId");
                dt_TopicCount.Columns.Add("BranchName");
                dt_TopicCount.Columns.Add("TopicCount");

                dt_TopicDetail.Columns.Add("RepositoryId");
                dt_TopicDetail.Columns.Add("TopicPath");

                foreach (var info in infos)
                {
                    dt_TopicCount.Rows.Add(info.PartitionKey, info.BranchName, info.Topics.Count);
                    if(info.Topics != null && info.Topics.Count > 0)
                    {
                        foreach (string topicPath in info.Topics)
                        {
                            dt_TopicDetail.Rows.Add(info.PartitionKey, topicPath);
                        }
                    }
                }

                Dictionary<string, DataTable> paramDic_TopicCount = new Dictionary<string, DataTable>();
                paramDic_TopicCount.Add("GitRepoTopicCountTableParam?GitRepoTopicCountType", dt_TopicCount);

                Dictionary<string, DataTable> paramDic_TopicDetail = new Dictionary<string, DataTable>();
                paramDic_TopicDetail.Add("GitRepoTopicDetailTableParam?GitRepoTopicDetailType", dt_TopicDetail);

                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitRepoTopicCount", paramDic_TopicCount, false);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitRepoTopicDetail", paramDic_TopicDetail, true);
            }
        }

        async Task<List<string>> GetFileCountForExtension(GitHubRepository repo, string branchName, string extension)
        {
            var github = new GitHubClient(new ProductHeaderValue("OPSMetrics"));
            var token = new Credentials(repo.AuthToken);
            github.Credentials = token;

            //var repository = await github.Repository.Get(repo.Owner, repo.RepositoryName);
            //var tree = await github.GitDatabase.Tree.GetRecursive(repository.Owner.Login, repository.Name, branchName);
            var tree = await github.GitDatabase.Tree.GetRecursive(repo.Owner, repo.RepositoryName, branchName);

            int count = 0;
            List<string> topics = new List<string>();
            if (tree.Truncated == false)
            {
                var items = tree.Tree;
                foreach (var item in items)
                {
                    if (item.Type == TreeType.Blob)
                    {
                        var itemExtension = System.IO.Path.GetExtension(item.Path);
                        if (string.Equals(itemExtension, "." + extension, StringComparison.OrdinalIgnoreCase))
                        {
                            count++;
                            topics.Add(item.Path);
                        }
                    }
                }
            }
            else
            {
                // If 'truncated' is true, the number of items in the tree array exceeded GitHub's maximum limit.
                // We need to use the non-recursive method of fetching trees, and fetch one sub-tree at a time.
                //tree = await github.GitDatabase.Tree.Get(repository.Owner.Login, repository.Name, branchName);
                //topics = await TraverseTreeManually(tree, github, repository, extension);

                tree = await github.GitDatabase.Tree.Get(repo.Owner, repo.RepositoryName, branchName);
                topics = await TraverseTreeManually(tree, github, repo.Owner, repo.RepositoryName, extension);
            }

            return topics;
        }

        //async Task<List<string>> TraverseTreeManually(TreeResponse rootTree, GitHubClient github, Repository repository, string extension)
        async Task<List<string>> TraverseTreeManually(TreeResponse rootTree, GitHubClient github, string owner, string repoName, string extension)
        {
            Stack<TreeItem> treeItems = new Stack<TreeItem>();

            int count = 0;
            List<string> topics = new List<string>();
            var items = rootTree.Tree;
            foreach (var item in items)
            {
                if (item.Type == TreeType.Blob)
                {
                    var itemExtension = System.IO.Path.GetExtension(item.Path);
                    if (string.Equals(itemExtension, "." + extension, StringComparison.OrdinalIgnoreCase))
                    {
                        count++;
                        topics.Add(item.Path);
                    }
                }
                else if (item.Type == TreeType.Tree)
                {
                    treeItems.Push(item);
                }
            }

            while (treeItems.Count != 0)
            {
                var treeItem = treeItems.Pop();

                var newTreeResponse = await github.GitDatabase.Tree.Get(owner, repoName, treeItem.Sha);
                items = newTreeResponse.Tree;
                foreach (var item in items)
                {
                    if (item.Type == TreeType.Blob)
                    {
                        var itemExtension = System.IO.Path.GetExtension(item.Path);
                        if (string.Equals(itemExtension, "." + extension, StringComparison.OrdinalIgnoreCase))
                        {
                            count++;
                            topics.Add(item.Path);
                        }
                    }
                    else if (item.Type == TreeType.Tree)
                    {
                        treeItems.Push(item);
                    }
                }
            }

            return topics;
        }
    }
}
