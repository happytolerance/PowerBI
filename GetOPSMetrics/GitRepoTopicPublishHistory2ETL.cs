using Octokit;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using System.Runtime.Serialization;
using System.Web;
using DiffMatchPatch;

namespace Insight.BackendJobs.GetOPSMetrics
{
    class GitRepoTopicPublishHistory2ETL : AbstractETL
    {
        protected override object Extract()
        {
            if (SharedObject_Prod_All == null)
            {
                return null;
            }

            List<GitRepoTopicPublishRecord2> ret = new List<GitRepoTopicPublishRecord2>();
            List<GitHubRepository> repos = SharedObject_Prod_All as List<GitHubRepository>;

            DateTime start = DateTime.Parse("2010-01-01");
            InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
            var dataRow = InsightDBHelper.InsightDBHelper.ExecuteQuery("SELECT MAX(StartTime) FROM OPS_RepoTopicPublishRecords2 WITH (NOLOCK)");
            if (dataRow != null
                && dataRow.Length > 0
                && dataRow[0] != null
                && dataRow[0].ItemArray != null
                && dataRow[0].ItemArray.Length > 0
                && dataRow[0].ItemArray[0] != null
                && !string.IsNullOrEmpty(dataRow[0].ItemArray[0].ToString()))
            {
                start = DateTime.Parse(dataRow[0].ItemArray[0].ToString()).AddSeconds(1);
            }

            List<OPSBuildInfo> opsBuildInfo = new List<OPSBuildInfo>();
            string utcNowStr = DateTime.UtcNow.ToString("yyyyMMddHHmmss");
            foreach (var r in repos)
            {
                GetOPSBuildInfo(start.ToString("yyyyMMddHHmmss"), utcNowStr, r.PartitionKey, null, opsBuildInfo);
            }
            // Get publish records for a test repo that is used by PM for testing
            GetOPSBuildInfo(start.ToString("yyyyMMddHHmmss"), utcNowStr, "1f799a29-9d37-b5e7-3c64-8283809f281d", null, opsBuildInfo);


            foreach (var buildItem in opsBuildInfo)
            {
                GitHubRepository repo = null;
                foreach (var r in repos)
                {
                    if (r.Owner.Equals(buildItem.repository_account, StringComparison.OrdinalIgnoreCase)
                        && r.RepositoryName.Equals(buildItem.repository_name, StringComparison.OrdinalIgnoreCase))
                    {
                        repo = r;
                        break;
                    }
                }
                // Get publish records for a test repo that is used by PM for testing
                if (repo == null && !buildItem.repository_id.Equals("1f799a29-9d37-b5e7-3c64-8283809f281d", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }
                if (buildItem.repository_id.Equals("1f799a29-9d37-b5e7-3c64-8283809f281d", StringComparison.OrdinalIgnoreCase))
                {
                    repo = new GitHubRepository() { PartitionKey = "1f799a29-9d37-b5e7-3c64-8283809f281d", IsLocalization = false, GitRepositoryType = "Vso" };
                }


                if (buildItem.branch_name.Equals("live", StringComparison.OrdinalIgnoreCase)
                    || buildItem.branch_name.Equals("master", StringComparison.OrdinalIgnoreCase))
                {
                    if (string.IsNullOrEmpty(buildItem.change_log_url))
                    {
                        continue;
                    }
                    JavaScriptSerializer jsonSerializer = new JavaScriptSerializer();
                    string contentInJson = Util.GetJsonContent(buildItem.change_log_url);
                    OPSBuildInfo_ChangeLog change_log = jsonSerializer.Deserialize<OPSBuildInfo_ChangeLog>(contentInJson);

                    Dictionary<string, int[]> wordChangeCountPerFile = null;
                    if (repo.GitRepositoryType.Equals("GitHub", StringComparison.OrdinalIgnoreCase) && !repo.IsLocalization)
                    {
                        //Get redirect_url by calling change_log_url
                        string redirect_url = change_log.redirect_url;
                        wordChangeCountPerFile = GetWordChangeCountPerFile(redirect_url, repo.AuthToken);
                    }

                    var authorLogins = new HashSet<string>(from commit in change_log.commits select commit.committer_login_name);
                    var authorNames = new HashSet<string>(from commit in change_log.commits select commit.committer_name);

                    List<GitRepoTopicPublishContentUpdate> topics = new List<GitRepoTopicPublishContentUpdate>();
                    foreach (var file in change_log.files)
                    {
                        if (file.file_name.EndsWith(".md"))
                        {
                            int? deleteCount = null, insertCount = null;
                            if (wordChangeCountPerFile != null && wordChangeCountPerFile.Count != 0
                                && repo.GitRepositoryType.Equals("GitHub", StringComparison.OrdinalIgnoreCase) && !repo.IsLocalization)
                            {
                                if (wordChangeCountPerFile.ContainsKey(file.file_name))
                                {
                                    deleteCount = wordChangeCountPerFile[file.file_name][0];
                                    insertCount = wordChangeCountPerFile[file.file_name][1];
                                }
                            }
                            GitRepoTopicPublishContentUpdate contentUpdate = new GitRepoTopicPublishContentUpdate()
                            {
                                TopicPath = file.file_name,
                                Insertions = file.insertions,
                                Deletions = file.deletions,
                                Status = file.status,
                                DeletionsOfWord = deleteCount,
                                InsertionsOfWord = insertCount
                            };
                            topics.Add(contentUpdate);
                        }
                    }
                    ret.Add(new GitRepoTopicPublishRecord2()
                    {
                        PublishId = buildItem.id,
                        PartitionKey = repo.PartitionKey,
                        TopicPaths = topics,
                        Branch = buildItem.branch_name,
                        StartTime = buildItem.started_at == null ? (DateTime?)null : DateTime.Parse(buildItem.started_at),
                        EndTime = buildItem.ended_at == null ? (DateTime?)null : DateTime.Parse(buildItem.ended_at),
                        AuthorLogins = authorLogins,
                        AuthorNames = authorNames,
                        Status = buildItem.status
                    });
                }
                else
                {
                    var record = new GitRepoTopicPublishRecord2()
                    {
                        PublishId = buildItem.id,
                        PartitionKey = repo.PartitionKey,
                        TopicPaths = null,
                        Branch = buildItem.branch_name,
                        StartTime = buildItem.started_at == null ? (DateTime?)null : DateTime.Parse(buildItem.started_at),
                        EndTime = buildItem.ended_at == null ? (DateTime?)null : DateTime.Parse(buildItem.ended_at),
                        AuthorLogins = new HashSet<string>(),
                        AuthorNames = new HashSet<string>(),
                        Status = buildItem.status
                    };
                    ret.Add(record);
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
            List<GitRepoTopicPublishRecord2> records = obj as List<GitRepoTopicPublishRecord2>;
            Dictionary<string, DataTable> paramDic = new Dictionary<string, DataTable>();
            InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);

            using (DataTable dt = new DataTable())
            {
                dt.Columns.Add("PublishId");
                dt.Columns.Add("RepositoryId");
                dt.Columns.Add("Branch");
                dt.Columns.Add("StartTime");
                dt.Columns.Add("EndTime");
                dt.Columns.Add("AuthorLogins");
                dt.Columns.Add("Status");
                dt.Columns.Add("AuthorNames");

                foreach (var record in records)
                {
                    dt.Rows.Add(record.PublishId, record.PartitionKey, record.Branch,
                        record.StartTime, record.EndTime, string.Join(",", record.AuthorLogins), record.Status, string.Join(",", record.AuthorNames));
                }

                paramDic.Add("GitRepoTopicPublishRecordsTableParam?GitRepoTopicPublishRecordType2", dt);
            }

            using (DataTable dt = new DataTable())
            {
                dt.Columns.Add("PublishId");
                dt.Columns.Add("RepositoryId");
                dt.Columns.Add("TopicPath");
                dt.Columns.Add("Insertions");
                dt.Columns.Add("Deletions");
                dt.Columns.Add("Status");
                dt.Columns.Add("InsertionsOfWord");
                dt.Columns.Add("DeletionsOfWord");

                foreach (var record in records)
                {
                    if (record.TopicPaths != null)
                    {
                        foreach (var topicPath in record.TopicPaths)
                        {
                            dt.Rows.Add(record.PublishId, record.PartitionKey, topicPath.TopicPath, topicPath.Insertions, topicPath.Deletions, topicPath.Status, topicPath.InsertionsOfWord, topicPath.DeletionsOfWord);
                        }
                    }
                }

                paramDic.Add("GitRepoTopicPublishRelationshipTableParam?GitRepoTopicPublishRelationshipType2", dt);
            }

            InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitRepoTopicPublishRecords3", paramDic, true);
        }

        //private async Task<List<GitRepoTopicPublishRecord2>> GetRecordsForChangeLog(GitHubRepository repo, OPSBuildInfo buildInfo, OPSBuildInfo_ChangeLog changeLog)
        //{
        //    var ret = new List<GitRepoTopicPublishRecord2>();

        //    var github = new GitHubClient(new ProductHeaderValue("OPSMetrics"));
        //    var token = new Credentials(repo.AuthToken);
        //    github.Credentials = token;
        //    var repository = await github.Repository.Get(repo.Owner, repo.RepositoryName);

        //    var fileList = changeLog.files.Select(v => v.file_name).Where(v => v.EndsWith(".md")).ToList();
        //    foreach (var commitInfo in changeLog.commits)
        //    {
        //        var commit = await github.Repository.Commits.Get(repository.Owner.Login, repository.Name, commitInfo.commit_sha);

        //        var topics = commit.Files.Where(v => fileList.Contains(v.Filename)).ToList();
        //        foreach (var topic in topics)
        //        {
        //            AddToRecords(ret, repo.PartitionKey, buildInfo, topic.Filename, commit.Author.Id.ToString());
        //        }
        //    }

        //    return ret;
        //}

        //private void AddToRecords(List<GitRepoTopicPublishRecord2> records, string partitionKey, OPSBuildInfo buildInfo, string topicPath, string authorId)
        //{
        //    foreach (var record in records)
        //    {
        //        if (record.TopicPath.Equals(topicPath, StringComparison.OrdinalIgnoreCase))
        //        {
        //            if (authorId != null)
        //            {
        //                record.AuthorIds.Add(authorId);
        //            }

        //            return;
        //        }
        //    }

        //    var newRecord = new GitRepoTopicPublishRecord2()
        //    {
        //        PartitionKey = partitionKey,
        //        TopicPath = topicPath,
        //        StartTime = DateTime.Parse(buildInfo.started_at),
        //        EndTime = DateTime.Parse(buildInfo.ended_at),
        //        AuthorIds = authorId == null? new HashSet<string>() : new HashSet<string>{ authorId },
        //        Status = buildInfo.status
        //    };

        //    records.Add(newRecord);
        //}

        private void GetOPSBuildInfo(string start, string end, string repoId, string continuationToken, List<OPSBuildInfo> opsBuildInfo)
        {
            // TODO: switch to prod env when deploying
            //string host_sandbox = "op-build-sandbox2.azurewebsites.net";
            //string token_sandbox = "6aeea674-981e-4437-afcb-283a50609e73";

            string host_prod = "op-build-prod.azurewebsites.net";
            string token_prod = "0a32b5c2-6b98-46a8-b8bb-2ea631ae0c98";

            string url = string.Format("https://{0}/v2/Queries/Builds?start={1}&end={2}&count=100&repository={3}", host_prod, start, end, repoId);

            Uri uri = new Uri(url);
            HttpWebRequest httpWebRequest = WebRequest.Create(uri) as HttpWebRequest;
            if (httpWebRequest.CookieContainer == null)
            {
                httpWebRequest.CookieContainer = new CookieContainer();
            }

            httpWebRequest.CookieContainer.Add(new Cookie("X-OP-BuildUserToken", token_prod) { Domain = uri.Host });

            if (continuationToken != null)
            {
                httpWebRequest.Headers["X-OP-ContinuationToken"] = continuationToken;
            }

            using (HttpWebResponse response = httpWebRequest.GetResponse() as HttpWebResponse)
            {
                if (response != null && response.StatusCode == HttpStatusCode.OK)
                {
                    continuationToken = response.Headers["X-OP-continuationToken"];
                    using (Stream stream = response.GetResponseStream())
                    {
                        if (stream != null)
                        {
                            using (StreamReader sr = new StreamReader(stream))
                            {
                                JavaScriptSerializer jsonSerializer = new JavaScriptSerializer();
                                string contentInJson = sr.ReadToEnd();
                                opsBuildInfo.AddRange(jsonSerializer.Deserialize<List<OPSBuildInfo>>(contentInJson));

                                if (continuationToken != null)
                                {
                                    GetOPSBuildInfo(start, end, repoId, continuationToken, opsBuildInfo);
                                }
                            }
                        }
                    }
                }
            }
        }
        private string GetGitHubDiffContent(string url, string tokenFormatInHttpHeader)
        {
            HttpWebRequest httpWebRequest = WebRequest.Create(url) as HttpWebRequest;
            httpWebRequest.Accept = @"text/html, application/vnd.github.diff, */*";

            httpWebRequest.Headers["Authorization"] = "token " + tokenFormatInHttpHeader;
            httpWebRequest.Method = "GET";
            httpWebRequest.UserAgent = @"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko";
            using (HttpWebResponse response = httpWebRequest.GetResponse() as HttpWebResponse)
            {
                using (Stream stream = response.GetResponseStream())
                {
                    using (StreamReader sr = new StreamReader(stream))
                    {
                        return sr.ReadToEnd();
                    }
                }
            }
        }


        private int[] GetWordChangeCount(string patch)
        {
            //The lines starting with "-" or "+" are regarded as changing lines, ignoring the spaces before "-" or "+"
            Regex deleteReg = new Regex(@"^\s*[-]");
            Regex insertReg = new Regex(@"^\s*[+]");
            diff_match_patch dmp = new diff_match_patch();

            string deleteLines = "", insertLines = "";
            int deleteCount = 0, insertCount = 0;

            string[] lines = patch.Split('\n');
            foreach (string line in lines)
            {
                //Put all the delete lines together
                if (deleteReg.IsMatch(line)) deleteLines += line.Substring(line.IndexOf("-") + 1) + "\n";
                //Put all the insertion lines together
                if (insertReg.IsMatch(line)) insertLines += line.Substring(line.IndexOf("+") + 1) + "\n";
            }
            List<Diff> diffs = dmp.diff_wordMode(deleteLines, insertLines);
            foreach (Diff diff in diffs)
            {
                if (diff.operation.ToString().Equals("DELETE")) deleteCount += diff.text.Split(' ').Length - 1;
                if (diff.operation.ToString().Equals("INSERT")) insertCount += diff.text.Split(' ').Length - 1;
            }

            int[] ret = new int[2];
            ret[0] = deleteCount;
            ret[1] = insertCount;
            return ret;
        }

        private Dictionary<string, int[]> GetWordChangeCountPerFile(string redirect_url, string token)
        {
            Dictionary<string, int[]> ret = new Dictionary<string, int[]>();

            redirect_url = "https://api.github.com/repos" + redirect_url.Substring("https://github.com".Length);
            if (redirect_url.Contains("commit")) //Only one commit in this publish
            {
                redirect_url = redirect_url.Replace("commit", "commits");
            }
            string contentInJson = Util.GetJsonContent(redirect_url, token);
            JavaScriptSerializer jsonSerializer = new JavaScriptSerializer();
            FilePublishInfoList fileInfoList = jsonSerializer.Deserialize<FilePublishInfoList>(contentInJson);

            foreach (var file in fileInfoList.files)
            {
                if (file.filename.EndsWith(".md"))
                {
                    string patch = "";

                    if (file.changes != 0) //Exist changes on this file
                    {
                        //Finding the content changed
                        if (!string.IsNullOrEmpty(file.patch))
                        {
                            patch = file.patch;
                        }
                        else //This case happens when the number of deletion lines or insertion lines is too large
                        {
                            string diffContent = GetGitHubDiffContent(redirect_url, token);
                            string[] contentOfFiles = Regex.Split(diffContent, "diff --git");
                            foreach (string contentOfFile in contentOfFiles)
                            {
                                if (contentOfFile.Contains(file.filename))
                                {
                                    patch = contentOfFile.Substring(contentOfFile.IndexOf("@@"));
                                    break;
                                }
                            }
                        }
                        ret.Add(file.filename, GetWordChangeCount(patch));
                    }
                }
            }
            return ret;
        }
    }
}
