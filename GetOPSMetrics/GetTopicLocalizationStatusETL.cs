using Octokit;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Insight.BackendJobs.GetOPSMetrics
{
    class GetTopicLocalizationStatusETL : AbstractETL
    {
        protected override object Extract()
        {
            List<GitRepoTopicLocalizationStatus> ret = new List<GitRepoTopicLocalizationStatus>();

            InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
            DataRow[] dataRow = InsightDBHelper.InsightDBHelper.ExecuteQuery("SELECT * FROM (SELECT RepositoryId, GitRepositoryName from OPS_Repositories_LocOnly WITH (NOLOCK)) Ex UNION "
                + "(SELECT R.RepositoryId, GitRepositoryName FROM OPS_Repositories R INNER JOIN OPS_Repositories_live L WITH (NOLOCK) "
                + "ON R.RepositoryId = L.RepositoryId WHERE IsLocalization = 1)", true);

            //Key:Localization Repo Name; Value: Repo ID
            Dictionary<string, string> dic = new Dictionary<string, string>();
            Dictionary<string, LocRepoFromAmbientConfiguration> format = Util.GetRepoInfoFromAmbientConfiguration();
            if (dataRow != null && dataRow.Length > 0)
            {
                foreach (DataRow row in dataRow)
                {
                    if (!dic.ContainsKey(row.ItemArray[0].ToString()))
                        dic.Add(row.ItemArray[1].ToString(), row.ItemArray[0].ToString());
                }
            }

            string token = Util.GetGithubToken("MIMDocs-pr.cs-cz");
            foreach (KeyValuePair<string, LocRepoFromAmbientConfiguration> pair in format)
            {
                string repoName = pair.Key;
                LocRepoFromAmbientConfiguration value = pair.Value;
                if (value.sourceRepo == null || value.sourceRepo == null) continue;
                if (repoName.Equals("win-cpub-itpro-docs")) token = Util.GetGithubToken("win-cpub-itpro-docs");

                //Get locale list
                string[] masterLocales = Util.GetLocalizationList(repoName, "master", token, format);
                string[] liveLocales = Util.GetLocalizationList(repoName, "live", token, format);

                //master branch
                if (masterLocales != null && masterLocales.Length != 0)
                {
                    foreach (string formatLocale in masterLocales)
                    {
                        string localizatonRepoName = Util.ComposeLocalizationRepoName(repoName, formatLocale, format);
                        if (dic.ContainsKey(localizatonRepoName))
                        {
                            string[] localizationInfo = Util.GetLocalizationInfo(localizatonRepoName, format);
                            if (localizationInfo == null || localizationInfo.Length != 2 || string.IsNullOrEmpty(localizationInfo[0]) || string.IsNullOrEmpty(localizationInfo[1])) continue;
                            string locale = localizationInfo[1];
                            string repoId = dic[localizatonRepoName];
                            ret.AddRange(ParseOLData(repoId, localizatonRepoName, "master", locale, token, value));
                        }
                    }
                }

                //live branch
                if (liveLocales != null && liveLocales.Length != 0)
                {
                    foreach (string formatLocale in liveLocales)
                    {
                        string localizatonRepoName = Util.ComposeLocalizationRepoName(repoName, formatLocale, format);
                        if (dic.ContainsKey(localizatonRepoName))
                        {
                            string[] localizationInfo = Util.GetLocalizationInfo(localizatonRepoName, format);
                            if (localizationInfo == null || localizationInfo.Length != 2 || string.IsNullOrEmpty(localizationInfo[0]) || string.IsNullOrEmpty(localizationInfo[1])) continue;
                            string locale = localizationInfo[1];
                            string repoId = dic[localizatonRepoName];
                            ret.AddRange(ParseOLData(repoId, localizatonRepoName, "live", locale, token, value));
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
            List<GitRepoTopicLocalizationStatus> status = obj as List<GitRepoTopicLocalizationStatus>;

            using (DataTable dt = new DataTable())
            {
                dt.Columns.Add("RepositoryId");
                dt.Columns.Add("Locale");
                dt.Columns.Add("Branch");
                dt.Columns.Add("TopicPath");
                dt.Columns.Add("HandoffDateTime");
                dt.Columns.Add("HandbackDateTime");

                foreach (var entry in status)
                {
                    dt.Rows.Add(entry.PartitionKey, entry.Locale, entry.Branch, entry.TopicPath, entry.HandoffDateTime, entry.HandbackDateTime);
                }

                Dictionary<string, DataTable> paramDic = new Dictionary<string, DataTable>();
                paramDic.Add("GitRepoLocalizationStatusTableParam?GitRepoLocalizationStatusType", dt);

                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitRepoLocalizationStatus", paramDic, true);
            }
        }

        private List<GitRepoTopicLocalizationStatus> ParseOLData(string repoId, string repoName, string branch, string locale, string token, LocRepoFromAmbientConfiguration value)
        {
            List<GitRepoTopicLocalizationStatus> ret = new List<GitRepoTopicLocalizationStatus>();

            LocRepo targetRepo = value.targetRepo;
            LocRepo handbackRepo = value.handbackRepo;

            JavaScriptSerializer j = new JavaScriptSerializer();
            j.MaxJsonLength = int.MaxValue;
            string url = "https://raw.githubusercontent.com/" + targetRepo.Owner + "/" + repoName + "/" + branch + "/.translation-state";

            try
            {
                string jsonContent = Util.GetJsonContent(url, token);
                var pair = j.DeserializeObject(jsonContent) as IDictionary<string, object>;
                ParseJsonContent(ret, pair, repoId, locale, branch);
                return ret;

                //#region TO DO: Change to use get data API
                //// 1. Get all commits for repositoty
                //var modelSHAs = new List<string>();
                //var commits = await github.Repository.Commits.GetAll(repository.Owner.Login, repository.Name);
                //foreach (var commit in commits)
                //{
                //    modelSHAs.Add(commit.Sha);
                //}
                //// 2. Get all references
                //var refs = github.GitDatabase.Reference.GetAll(repository.Owner.Login, repository.Name).Result;
                //if (null == refs || 0 == refs.Count)
                //    return ret;
                //var currentRefSet = refs.Where(x => x.Ref.EndsWith(branch, StringComparison.OrdinalIgnoreCase));
                //if (null == currentRefSet || 0 == currentRefSet.Count())
                //    return ret;
                //var currentRef = currentRefSet.ElementAt(0).Object.Sha;
                //// 3. Get content using get data API
                //// currentRef is not correct in this scenario. We must find the SHA of the blob. Actually, the SHA will be returned in API github.Repository.Content.GetAllContents(string, string, string, string).
                //var blobContent = github.GitDatabase.Blob.Get(repository.Owner.Login, repository.Name, currentRef).Result;
                //var convertedContents = System.Text.Encoding.Default.GetString(Convert.FromBase64String(blobContent.Content));
                //#endregion
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("404"))
                {
                    try
                    {
                        //ol-handback/<taget-repo-owner>/<target-repo-name>/<target-repo-branch>/olsysfiles/.translation-state
                        url = "https://raw.githubusercontent.com/" + handbackRepo.Owner + "/" + handbackRepo.Name + "/" + branch + "/ol-handback/"
                            + targetRepo.Owner + "/" + repoName + "/" + targetRepo.WorkingBranch + "/olsysfiles/.translation-state";

                        string jsonContent = Util.GetJsonContent(url, token);
                        var pair = j.DeserializeObject(jsonContent) as IDictionary<string, object>;
                        ParseJsonContent(ret, pair, repoId, locale, branch);
                        return ret;
                    }
                    catch (Exception innerEx)
                    {
                        if (innerEx.Message.Contains("404")) return ret;
                        throw innerEx;
                    }
                }
                throw ex;
            }
        }

        private void ParseJsonContent(List<GitRepoTopicLocalizationStatus> ret, IDictionary<String, object> pair, string repoId, string locale, string branch)
        {
            DateTime? latestHandoffTime = DateTime.Parse("2010-01-01");
            DateTime? latestHandbackTime = DateTime.Parse("2010-01-01");

            InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
            var dataRow = InsightDBHelper.InsightDBHelper.ExecuteQuery(string.Format("select max(HandoffDateTime), max(HandbackDateTime) from OPS_LocalizationStatus L with (nolock) " +
                "inner join OPS_RepoTopics T with(nolock) on L.TopicId = T.TopicId where T.RepositoryId = '{0}'", repoId),true);
            if (dataRow != null
                && dataRow.Length > 0
                && dataRow[0] != null
                && dataRow[0].ItemArray != null
                && dataRow[0].ItemArray.Length > 0)
            {
                latestHandoffTime = DateTime.Parse(dataRow[0].ItemArray[0].ToString()).AddSeconds(1);
                if (dataRow[0].ItemArray[1] != null && !string.IsNullOrEmpty(dataRow[0].ItemArray[1].ToString()))
                    latestHandbackTime = DateTime.Parse(dataRow[0].ItemArray[1].ToString()).AddSeconds(1);
            }

            var values = (pair["Items"] as Dictionary<string, object>).Values;
            foreach (object value in values)
            {
                Dictionary<string, object> olDataUnit = value as Dictionary<string, object>;
                string[] fileArray = Array.ConvertAll(olDataUnit["FilePathList"] as Object[], x => x.ToString());
                if (fileArray == null)
                {
                    continue;
                }
                List<string> topicList = fileArray.ToList().Where(v => v.EndsWith(".md")).ToList();
                foreach (var topic in topicList)
                {
                    DateTime? handoffDateTime = DateTime.Parse(olDataUnit["HandoffDateTime"].ToString());
                    if (handoffDateTime == DateTime.MinValue)
                    {
                        handoffDateTime = null;
                    }

                    DateTime? handbackDateTime = DateTime.Parse(olDataUnit["HandbackDateTime"].ToString());
                    if (handbackDateTime == DateTime.MinValue)
                    {
                        handbackDateTime = null;
                    }

                    if (handoffDateTime > latestHandoffTime || handbackDateTime > latestHandbackTime)
                    {
                        ret.Add(new GitRepoTopicLocalizationStatus()
                        {
                            PartitionKey = repoId,
                            Locale = locale,
                            Branch = branch,
                            TopicPath = topic.Replace("\\", "/"),
                            HandoffDateTime = handoffDateTime,
                            HandbackDateTime = handbackDateTime
                        });
                    }
                }
            }
        }
    }
}
