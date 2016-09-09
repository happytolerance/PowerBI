using Octokit;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Insight.BackendJobs.GetOPSMetrics
{
    class GitRepoTopicPublishHistoryETL : AbstractETL
    {
        protected override object Extract()
        {
            if (SharedObject_Prod_GitHub == null)
            {
                return null;
            }

            List<GitRepoTopicPublishRecord> ret = new List<GitRepoTopicPublishRecord>();
            List<GitHubRepository> repos = SharedObject_Prod_GitHub as List<GitHubRepository>;

            foreach (var repo in repos)
            {
                if(repo.Branches == null || repo.Branches.Count == 0)
                {
                    continue;
                }

                var task = GetPublishHistory(repo);
                ret.AddRange(task.Result);
            }

            return ret;
        }

        protected override object Transform(object obj)
        {
            return obj;
        }

        protected override void Load(object obj)
        {
            List<GitRepoTopicPublishRecord> records = obj as List<GitRepoTopicPublishRecord>;
            InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
            var dataRow = InsightDBHelper.InsightDBHelper.ExecuteQuery("SELECT MAX(PublishDateTime) FROM OPS_RepoTopicPublishRecords WITH (NOLOCK)");
            if (dataRow != null)
            {
                DateTime? lastPublishDataTime = dataRow[0].ItemArray[0] as DateTime?;
                records = records.Where(v => DateTime.Compare(v.PublishDateTime, lastPublishDataTime.GetValueOrDefault()) >= 0).ToList();
            }

            using (DataTable dt = new DataTable())
            {
                dt.Columns.Add("RepositoryId");
                dt.Columns.Add("PullRequestNumber");
                dt.Columns.Add("TopicPath");
                dt.Columns.Add("PublishDateTime");
                dt.Columns.Add("AuthorIds");

                foreach (var record in records)
                {
                    dt.Rows.Add(record.PartitionKey, record.PullRequestNumber, record.TopicPath, record.PublishDateTime, string.Join(",", record.AuthorIds));
                }

                Dictionary<string, DataTable> paramDic = new Dictionary<string, DataTable>();
                paramDic.Add("GitRepoTopicPublishRecordsTableParam?GitRepoTopicPublishRecordType", dt);

                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitRepoTopicPublishRecords", paramDic, true);
            }
        }

        private async Task<List<GitRepoTopicPublishRecord>> GetPublishHistory(GitHubRepository repo)
        {
            var github = new GitHubClient(new ProductHeaderValue("OPSMetrics"));
            var token = new Credentials(repo.AuthToken);
            github.Credentials = token;

            var repository = await github.Repository.Get(repo.Owner, repo.RepositoryName);

            var pullRequestRequest = new PullRequestRequest() { State = ItemState.Closed, Base = "live" };
            var pullRequests = await github.Repository.PullRequest.GetAllForRepository(repository.Owner.Login, repository.Name, pullRequestRequest);

            List<GitRepoTopicPublishRecord> history = new List<GitRepoTopicPublishRecord>();

            foreach (var pullRequest in pullRequests)
            {
                var merged = await github.PullRequest.Merged(repository.Owner.Login, repository.Name, pullRequest.Number);
                if (merged)
                {
                    List<GitRepoTopicPublishRecord> records = new List<GitRepoTopicPublishRecord>();

                    var files = await github.Repository.PullRequest.Files(repository.Owner.Login, repository.Name, pullRequest.Number);
                    var fileNames = files.Where(file => file.FileName.EndsWith(".md")).Select(file => file.FileName).ToList();

                    var commitInfos = await github.PullRequest.Commits(repository.Owner.Login, repository.Name, pullRequest.Number);

                    foreach (var commitInfo in commitInfos)
                    {
                        var commit = await github.Repository.Commits.Get(repository.Owner.Login, repository.Name, commitInfo.Sha);
                        if (commit.Parents.Count < 2)   // Ignore merge commits
                        {
                            var updatedFiles = commit.Files;
                            var updatedTopics = updatedFiles.Where(v => fileNames.Contains(v.Filename)).ToList();

                            foreach (var updatedTopic in updatedTopics)
                            {
                                AddToRecords(records, repo.PartitionKey, pullRequest.Number, updatedTopic.Filename,
                                    pullRequest.MergedAt.Value.DateTime, commit.Author == null ? null : commit.Author.Id.ToString());   // Merged time of PR as publish time instead of commit time

                                if (updatedTopic.PreviousFileName != null)  // If the topic is renamed
                                {
                                    AddToRecords(records, repo.PartitionKey, pullRequest.Number, updatedTopic.PreviousFileName,
                                        pullRequest.MergedAt.Value.DateTime, commit.Author == null ? null : commit.Author.Id.ToString());
                                }
                            }
                        }
                    }

                    history.AddRange(records);
                }
            }

            return history;
        }

        private void AddToRecords(List<GitRepoTopicPublishRecord> records, string partitionKey, int pullRequestNumber, string topicPath,
            DateTime publishDateTime, string authorId)
        {
            foreach (var record in records)
            {
                // If the topic is already added in the records for a PR, add the author to the author list
                if (record.TopicPath.Equals(topicPath, StringComparison.OrdinalIgnoreCase))
                {
                    if (authorId != null)
                    {
                        record.AuthorIds.Add(authorId);
                    }

                    return;
                }
            }

            var newRecord = new GitRepoTopicPublishRecord()
            {
                PartitionKey = partitionKey,
                PullRequestNumber = pullRequestNumber,
                TopicPath = topicPath,
                PublishDateTime = publishDateTime,
                AuthorIds = authorId == null ? new HashSet<string>() : new HashSet<string> { authorId }
            };

            records.Add(newRecord);
        }
    }
}
