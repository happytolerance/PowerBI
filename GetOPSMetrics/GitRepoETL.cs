using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Web.Script.Serialization;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Insight.BackendJobs.GetOPSMetrics
{
    public class GitRepoETL : AbstractETL
    {
        protected override object Extract()
        {
            List <GitHubRepository> ret = new List<GitHubRepository>();

            string BuildStorageConnectionString = configManager.GetConfig("BackendJobs", "OPSBuildStorageConnectionString");
            HashSet<string> OPGitRepoAccountWhitelist = new HashSet<string>(configManager.GetConfig("BackendJobs", "OPGitRepoAccountWhitelist").Split(';'));

            var StorageAccount = CloudStorageAccount.Parse(BuildStorageConnectionString);
            var TableClient = StorageAccount.CreateCloudTableClient();
            var repoTable = TableClient.GetTableReference("RepositoryTableEntity");
            var userTable = TableClient.GetTableReference("UserTableEntity");

            TableQuery<RepositoryTableEntity> query = new TableQuery<RepositoryTableEntity>();

            foreach (RepositoryTableEntity repo in repoTable.ExecuteQuery(query))
            {
                TableQuery<UserTableEntity> userQuery = new TableQuery<UserTableEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, repo.CreatedBy));
                UserTableEntity user = userTable.ExecuteQuery(userQuery).FirstOrDefault();

                if (user != null)
                {
                    ret.Add(new GitHubRepository()
                    {
                        Owner = repo.GitRepositoryAccount,
                        RepositoryName = repo.GitRepositoryName,
                        RepositoryUrl = repo.GitRepositoryUrl,
                        AuthToken = user.AccessToken,
                        PartitionKey = repo.PartitionKey,
                        Timestamp = repo.Timestamp.ToString("yyyy-MM-dd"),
                        GitRepositoryType = repo.GitRepositoryType
                    });
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

            List<GitHubRepository> repos = obj as List<GitHubRepository>;
            Dictionary<string, LocRepoFromAmbientConfiguration> repoInfo = Util.GetRepoInfoFromAmbientConfiguration();

            using (DataTable dt = new DataTable(), dt1 = new DataTable())
            {
                dt.Columns.Add("RepositoryId");
                dt.Columns.Add("GitRepositoryAccount");
                dt.Columns.Add("GitRepositoryName");
                dt.Columns.Add("GitRepositoryUrl");
                dt.Columns.Add("CreatedAt");
                dt.Columns.Add("Repository");
                dt.Columns.Add("IsLocalization");
                dt.Columns.Add("Locale");

                foreach (GitHubRepository repo in repos)
                {
                    int isLocalization = 1;
                    string repository = "", locale = "";
                    string[] localeInfo = Util.GetLocalizationInfo(repo.RepositoryName, repoInfo);
                    if (localeInfo != null && localeInfo.Length == 2 && !string.IsNullOrEmpty(localeInfo[0]) && !string.IsNullOrEmpty(localeInfo[1]))
                    {
                        repository = localeInfo[0];
                        locale = localeInfo[1];
                    }
                    if (locale.Equals("en-us"))
                    {
                        isLocalization = 0;
                        repo.IsLocalization = false;
                    }
                    if (locale.Length == 4) locale = locale.Insert(2, "-");
                    dt.Rows.Add(repo.PartitionKey, repo.Owner, repo.RepositoryName, repo.RepositoryUrl, repo.Timestamp, repository, isLocalization, locale);
                }

                //Add localization repositories that don't have en-us repository.
                dt1.Columns.Add("RepositoryId");
                dt1.Columns.Add("GitRepositoryAccount");
                dt1.Columns.Add("GitRepositoryName");
                dt1.Columns.Add("GitRepositoryUrl");
                dt1.Columns.Add("CreatedAt");
                dt1.Columns.Add("Repository");
                dt1.Columns.Add("IsLocalization");
                dt1.Columns.Add("Locale");

                string token = Util.GetGithubToken("MIMDocs-pr.cs-cz");
                string[] repositoryException = configManager.GetConfig("BackendJobs", "RepositoryException").Split(';');

                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                DataRow[] dataRow = InsightDBHelper.InsightDBHelper.ExecuteQuery(
                    string.Format("SELECT DISTINCT GitRepositoryName FROM OPS_Repositories_LocOnly WITH(NOLOCK)"),true);
                HashSet<string> locRepoSet = new HashSet<string>();
                if (dataRow != null && dataRow.Length > 0)
                {
                    foreach (DataRow row in dataRow) locRepoSet.Add(row.ItemArray[0].ToString());
                }

                foreach (string repo in repositoryException)
                {
                    LocRepoFromAmbientConfiguration value = repoInfo[repo];
                    if (value.sourceRepo == null || value.targetRepo == null) continue;
                    LocRepo sourceRepo = value.sourceRepo;
                    LocRepo targetRepo = value.targetRepo;

                    string[] items = Util.GetLocalizationList(repo, "master", token, repoInfo);
                    if (items == null || items.Length == 0) continue;
                    foreach (string locale in items)
                    {
                        string localizatonRepoName = Util.ComposeLocalizationRepoName(repo, locale, repoInfo);
                        if (locRepoSet.Count == 0 || !locRepoSet.Contains(localizatonRepoName))
                        {
                            string repoId = System.Guid.NewGuid().ToString("D");
                            string remoteUrl = sourceRepo.RemoteUrl;
                            string repoUrl = remoteUrl.Substring(0, remoteUrl.LastIndexOf("/")) + "/" + localizatonRepoName;
                            dt1.Rows.Add(repoId, targetRepo.Owner, localizatonRepoName, repoUrl, "", repo, 1, locale);
                        }
                    }
                }

                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);

                Dictionary<string, DataTable> paramDic1 = new Dictionary<string, DataTable>();
                paramDic1.Add("GitRepoTableParam?GitRepoType", dt1);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitRepoLocOnly", paramDic1);

                Dictionary<string, DataTable> paramDic = new Dictionary<string, DataTable>();
                paramDic.Add("GitRepoTableParam?GitRepoType", dt);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitRepo", paramDic, true, 600);
            }
            
            // Get prod repo
            InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
            DataRow[] prodRepoRow = InsightDBHelper.InsightDBHelper.ExecuteQuery("SELECT DISTINCT RepositoryId FROM OPS_Repositories_live WITH (NOLOCK)", true);
            if (prodRepoRow == null || prodRepoRow.Length == 0)
            {
                return;
            }

            List<DataRow> prodRepoList = prodRepoRow.ToList();
            List<GitHubRepository> prod_all_tmp = repos.Where(v => prodRepoList.Exists(t => t.ItemArray[0].ToString().Equals(v.PartitionKey, StringComparison.OrdinalIgnoreCase))).ToList();

            string testRepositoryIdString = configManager.GetConfig("BackendJobs", "TestRepositoryId");
            string[] testIdList = testRepositoryIdString.Split(';');
            //Add two test repositoryIds
            prod_all_tmp.AddRange(repos.Where(v => Array.Exists(testIdList, t => t.Equals(v.PartitionKey, StringComparison.OrdinalIgnoreCase))).ToList());

            SharedObject_Prod_All = prod_all_tmp;
            SharedObject_Prod_GitHub = prod_all_tmp.Where(v => v.GitRepositoryType.Equals("GitHub", StringComparison.OrdinalIgnoreCase)).ToList();
            SharedObject_Prod_VSO = prod_all_tmp.Where(v => v.GitRepositoryType.Equals("Vso", StringComparison.OrdinalIgnoreCase)).ToList();

            return;
        }
    }
}
