using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.IO;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Web.Script.Serialization;

//using Microsoft.Hadoop.Client;
//using Microsoft.WindowsAzure.Management.HDInsight;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;

using Insight.BackendJobs.Utilities;

namespace Insight.BackendJobs.GetOPSMetrics
{
    public static class Util
    {
        public static object CallGitHubAPI<T>(string url, string authToken) where T : class
        {
            string uriStr = string.Format(url);
            HttpWebRequest httpWebRequest = ComposeWebRequest(uriStr, authToken);

            string contentInJson;
            JavaScriptSerializer jsonSerializer = new JavaScriptSerializer();
            try
            {
                using (HttpWebResponse response = httpWebRequest.GetResponse() as HttpWebResponse)
                {
                    if (response != null && response.StatusCode == HttpStatusCode.MovedPermanently)
                    {
                        var redirectWebRequest = ComposeWebRequest(response.Headers["Location"], authToken);
                        using (HttpWebResponse redirectResponse = redirectWebRequest.GetResponse() as HttpWebResponse)
                        {
                            using (Stream stream = redirectResponse.GetResponseStream())
                            {
                                if (stream != null)
                                {
                                    using (StreamReader sr = new StreamReader(stream))
                                    {
                                        contentInJson = sr.ReadToEnd();
                                        return jsonSerializer.Deserialize<T>(contentInJson);
                                    }
                                }
                            }
                        };
                    }
                    else
                        if (response != null && response.StatusCode == HttpStatusCode.OK)
                        {
                            using (Stream stream = response.GetResponseStream())
                            {
                                if (stream != null)
                                {
                                    using (StreamReader sr = new StreamReader(stream))
                                    {
                                        contentInJson = sr.ReadToEnd();
                                        return jsonSerializer.Deserialize<T>(contentInJson);
                                    }
                                }
                            }
                        }
                }
            }
            catch (Exception e)
            {
                if (e.Message.Contains("(404) Not Found."))
                {
                    // Ignore.
                }
                else
                {
                    throw e;
                }
            }

            return null;
        }

        private static HttpWebRequest ComposeWebRequest(string uriStr, string authToken)
        {
            HttpWebRequest httpWebRequest = WebRequest.Create(uriStr) as HttpWebRequest;

            httpWebRequest.Accept = @"text/html, application/xhtml+xml, */*";
            httpWebRequest.Headers["Authorization"] = "token " + authToken;

            httpWebRequest.ProtocolVersion = HttpVersion.Version11;
            httpWebRequest.Method = "GET";
            httpWebRequest.UserAgent = @"Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko";
            httpWebRequest.AllowAutoRedirect = false;

            return httpWebRequest;
        }

        public static object CallGitVSOAPI<T>(string url) where T : class
        {
            // Call build API to refresh token
            if (GenVSOTokenByCallingBuildAPI())
            {
                string filter1 = TableQuery.GenerateFilterCondition("Name", QueryComparisons.Equal, "Nanxuan Xu");
                string filter2 = TableQuery.GenerateFilterCondition("GitRepositoryType", QueryComparisons.Equal, "Vso");
                TableQuery<UserTableEntity> userQuery = new TableQuery<UserTableEntity>().Where(TableQuery.CombineFilters(filter1, TableOperators.And, filter2));

                ConfigManager configManager = new ConfigManager();
                string BuildStorageConnectionString = configManager.GetConfig("BackendJobs", "OPSBuildStorageConnectionString");
                var StorageAccount = CloudStorageAccount.Parse(BuildStorageConnectionString);
                var TableClient = StorageAccount.CreateCloudTableClient();
                var userTable = TableClient.GetTableReference("UserTableEntity");
                UserTableEntity user = userTable.ExecuteQuery(userQuery).FirstOrDefault();
                if (user == null)
                {
                    return null;
                }
                string token = user.AccessToken; //RefreshToken?

                HttpWebRequest httpWebRequest = WebRequest.Create(new Uri(url)) as HttpWebRequest;
                httpWebRequest.Method = "Get";
                httpWebRequest.Credentials = new NetworkCredential("Bearer", token);
                httpWebRequest.Headers.Add("Authorization", "Bearer " + token);
                httpWebRequest.Accept = "application/json";

                string contentInJson;
                JavaScriptSerializer jsonSerializer = new JavaScriptSerializer();
                try
                {
                    using (HttpWebResponse response = httpWebRequest.GetResponse() as HttpWebResponse)
                    {
                        if (response != null && response.StatusCode == HttpStatusCode.OK)
                        {
                            using (Stream stream = response.GetResponseStream())
                            {
                                if (stream != null)
                                {
                                    using (StreamReader sr = new StreamReader(stream))
                                    {
                                        contentInJson = sr.ReadToEnd();
                                        return jsonSerializer.Deserialize<T>(contentInJson);
                                    }
                                }
                            };
                        }
                    }
                }
                catch (Exception e)
                {
                    if (e.Message.Contains("(404) Not Found."))
                    {
                        return null;
                    }
                    throw e;
                }
            }
            return null;
        }

        private static bool GenVSOTokenByCallingBuildAPI()
        {
            // TODO: switch to prod env when deploying
            //string host_sandbox = "op-build-sandbox2.azurewebsites.net";
            //string token_sandbox = "6aeea674-981e-4437-afcb-283a50609e73";

            string host_prod = "op-build-prod.azurewebsites.net";
            string token_prod = "37050bb2-13e8-4c76-9a3b-eb58df607c1a";//"0a32b5c2-6b98-46a8-b8bb-2ea631ae0c98";

            string url = string.Format("https://{0}/v1/Queries/Repositories?scope=all", host_prod);

            Uri uri = new Uri(url);
            HttpWebRequest httpWebRequest = WebRequest.Create(uri) as HttpWebRequest;
            if (httpWebRequest.CookieContainer == null)
            {
                httpWebRequest.CookieContainer = new CookieContainer();
            }
            httpWebRequest.CookieContainer.Add(new Cookie("X-OP-BuildUserToken", token_prod) { Domain = uri.Host });

            using (HttpWebResponse response = httpWebRequest.GetResponse() as HttpWebResponse)
            {
                if (response != null && response.StatusCode == HttpStatusCode.OK) // Add retry here
                {
                    return true;
                }
            }
            return false;
        }
        /*
        public static string RunHiveQuery(
            string hql, 
            string dateStr, 
            string subscriptionID, 
            string thumbprint, 
            string clusterName,
            string storageAccountName, 
            string storageAccountKey, 
            string storageContainerName)
        {
            string ret = "";
            string subFolderName = "DailyOPSPublish_" + dateStr;
            HiveJobCreateParameters hiveJobDefinition = CreateHiveJobParam(hql, subFolderName, storageAccountName, storageAccountKey, storageContainerName);

            // Get the certificate object from certificate store
            X509Store store = new X509Store(StoreLocation.LocalMachine);
            store.Open(OpenFlags.ReadOnly);
            X509Certificate2 cert = store.Certificates.Cast<X509Certificate2>().FirstOrDefault(item => item.Thumbprint.Equals(thumbprint, StringComparison.OrdinalIgnoreCase));
            JobSubmissionCertificateCredential creds = new JobSubmissionCertificateCredential(new Guid(subscriptionID), cert, clusterName);

            // Submit the Hive job
            var jobClient = JobSubmissionClientFactory.Connect(creds);
            JobCreationResults jobResults = jobClient.CreateHiveJob(hiveJobDefinition);
            //AddJobId(dbQueueId, jobResults.JobId);

            // Wait for the job to complete
            string errorHiveMsg = "";

            // Hadoop SDK sometimes cannot get hadoop job status/running result when job finishes, instead, exception is thrown.
            // In such situation, directly pull the result instead leveraging the SDK any more.
            bool jobSucceeded = false;
            try
            {
                jobSucceeded = WaitForJobCompletion(jobResults, jobClient, hiveJobDefinition, out errorHiveMsg);
            }
            catch (Exception ex)
            {
                if (ex.Message.StartsWith("Request failed with code:BadGateway", StringComparison.OrdinalIgnoreCase))
                {
                    ret = ReadFromWASB(subFolderName, storageAccountName, storageAccountKey, storageContainerName);
                    return ret;
                }
            }

            if (!jobSucceeded)
            {
                throw new Exception(errorHiveMsg);
            }

            // Get hive job output
            using (System.IO.Stream stream = jobClient.GetJobOutput(jobResults.JobId))
            {
                using (StreamReader sr = new StreamReader(stream))
                {
                    ret = sr.ReadToEnd();
                }
            }

            return ret;
        }

        private static HiveJobCreateParameters CreateHiveJobParam(string hql, string subFolderName, string storageAccountName, string storageAccountKey, string storageContainerName)
        {
            HiveJobCreateParameters ret = new HiveJobCreateParameters()
            {
                JobName = subFolderName,
                StatusFolder = string.Format("/CapsInsightUserQueryBlob/{0}", subFolderName),
            };

            if (hql.Contains('%') || hql.Contains('\r') || hql.Contains('\n'))
            {
                StoreHQLToWASB(hql, subFolderName, storageAccountName, storageAccountKey, storageContainerName);
                ret.File = string.Format("/CapsInsightUserQueryBlob/{0}/{1}", subFolderName, subFolderName + ".hql");
                ret.RunAsFileJob = true;
            }
            else
            {
                ret.Query = hql;
            }

            return ret;
        }

        private static void StoreHQLToWASB(string hql, string subFolderName, string storageAccountName, string storageAccountKey, string storageContainerName)
        {
            string personalFlowFolderFullName = string.Format("CapsInsightUserQueryBlob/{0}", subFolderName);
            string personalHQLFileFullName = string.Format("{0}/{1}", personalFlowFolderFullName, subFolderName + ".hql");
            StoreTextToWASB(hql, personalHQLFileFullName, storageAccountName, storageAccountKey, storageContainerName);
        }

        private static void StoreTextToWASB(string textContent, string fileName, string storageAccountName, string storageAccountKey, string storageContainerName)
        {
            StorageCredentials cred = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount acct = new CloudStorageAccount(cred, true);
            CloudBlobClient blobClient = acct.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(storageContainerName);
            CloudBlockBlob blob = container.GetBlockBlobReference(fileName);
            using (MemoryStream ms = new MemoryStream(Encoding.UTF8.GetBytes(textContent)))
            {
                blob.UploadFromStream(ms);
            }
        }

        private static string ReadFromWASB(string personalFolder, string storageAccountName, string storageAccountKey, string storageContainerName)
        {
            StorageCredentials cred = new StorageCredentials(storageAccountName, storageAccountKey);
            CloudStorageAccount acct = new CloudStorageAccount(cred, true);
            CloudBlobClient blobClient = acct.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(storageContainerName);

            string resultFileName = string.Format("CapsInsightUserQueryBlob/{0}/stdout", personalFolder);
            CloudBlockBlob blob = container.GetBlockBlobReference(resultFileName);
            return blob.DownloadText();
        }

        private static bool WaitForJobCompletion(JobCreationResults jobResults, IJobSubmissionClient client, HiveJobCreateParameters hiveJobDefinition, out string errorMsg)
        {
            JobDetails jobInProgress = client.GetJob(jobResults.JobId);
            DateTime start = DateTime.Now;
            int hangRetryIdx = 0;
            int failRetryIdx = 0;
            int failRetryLimit = 1;
            int hangTimedoutInMin = 60 * 23;
            int hangRetryLimit = 2;
            int[] HangRetryIntervalInSecScope = new int[] { 0, 15, 30, 45, 60, 75, 90, 105, 120, 135, 150, 165, 180 };
            errorMsg = "";
            while (jobInProgress.StatusCode != JobStatusCode.Completed)
            {
                if (jobInProgress.StatusCode == JobStatusCode.Running)
                {
                    if (DateTime.Now.Subtract(start) < TimeSpan.FromMinutes(hangTimedoutInMin))
                    {
                        Thread.Sleep(TimeSpan.FromSeconds(60));
                        jobInProgress = client.GetJob(jobInProgress.JobId);
                    }

                    // Assume job hang in this situation. Kill it and recreate.
                    else if (hangRetryIdx < hangRetryLimit)
                    {
                        client.StopJob(jobResults.JobId);
                        int retryInterval = new Random((int)DateTime.Now.Ticks).Next(0, HangRetryIntervalInSecScope.Length);
                        Thread.Sleep(TimeSpan.FromSeconds(retryInterval));
                        jobResults = client.CreateHiveJob(hiveJobDefinition);
                        //AddJobId(dbQueueId, jobResults.JobId);
                        jobInProgress = client.GetJob(jobResults.JobId);
                        start = DateTime.Now;
                        hangRetryIdx++;
                    }
                    else
                    {
                        client.StopJob(jobResults.JobId);
                        errorMsg = "Job always hang after several retries.";
                        return false;
                    }
                }
                else if (jobInProgress.StatusCode == JobStatusCode.Failed) // This should be corner case. Not happened yet until now.
                {
                    if (failRetryIdx < failRetryLimit)
                    {
                        client.StopJob(jobInProgress.JobId);
                        Thread.Sleep(TimeSpan.FromSeconds(60));
                        jobResults = client.CreateHiveJob(hiveJobDefinition);
                        jobInProgress = client.GetJob(jobResults.JobId);
                        failRetryIdx++;
                    }
                    else
                    {
                        client.StopJob(jobInProgress.JobId);
                        errorMsg = "The returned job status is Failed.";
                        return false;
                    }
                }
                else
                {
                    Thread.Sleep(TimeSpan.FromSeconds(60));
                    jobInProgress = client.GetJob(jobInProgress.JobId);
                }
            }

            return true;
        }
        */
        public static string GetJsonContent(string url, string tokenFormatInHttpHeader = "")
        {
            HttpWebRequest httpWebRequest = WebRequest.Create(url) as HttpWebRequest;
            httpWebRequest.Accept = @"text/html, application/xhtml+xml, */*";
            if (!string.IsNullOrEmpty(tokenFormatInHttpHeader))
            {
                httpWebRequest.Headers["Authorization"] = "token " + tokenFormatInHttpHeader;
            }
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

        public static Dictionary<string, LocRepoFromAmbientConfiguration> GetRepoInfoFromAmbientConfiguration()
        {
            Dictionary<string, LocRepoFromAmbientConfiguration> ret = new Dictionary<string, LocRepoFromAmbientConfiguration>();
            string url = "http://ambientconfiguration.blob.core.windows.net/openlocalization/openlocalization_prod.json";
            string jsonContent = Util.GetJsonContent(url);

            JavaScriptSerializer j = new JavaScriptSerializer();
            RepoListFromAmbientConfiguration repoList = j.Deserialize<RepoListFromAmbientConfiguration>(jsonContent) as RepoListFromAmbientConfiguration;

            foreach (LocRepoFromAmbientConfiguration repo in repoList.Repos)
            {
                string remoteUrl = repo.sourceRepo.RemoteUrl;
                string repoName = remoteUrl.Substring(remoteUrl.LastIndexOf("/") + 1);

                if (!string.IsNullOrEmpty(repo.targetRepo.Name))
                {
                    string regex = repo.targetRepo.Name;
                    if (regex.Contains("<target_locale_without_dash>"))
                    {
                        if (regex.Contains("-<target_locale_without_dash>-"))
                        {
                            repo.repoNameRegex = regex.Substring(0, regex.IndexOf("-<")) + "-[a-z]{4}-" + regex.Substring(regex.IndexOf(">-") + 2);
                            repo.localeRegex = "-[a-z]{4}-";
                        }
                        else
                        {
                            repo.repoNameRegex = regex.Substring(0, regex.IndexOf("-<")) + "-[a-z]{4}";
                            repo.localeRegex = "-[a-z]{4}";
                        }
                        repo.localStartIndex = 1;
                        repo.localLength = 4;
                    }
                }
                else
                {
                    //default separate by dot
                    repo.repoNameRegex = repoName + "\\.[a-z]{2}-[a-z]{2}";
                    repo.localeRegex = "\\.[a-z]{2}-[a-z]{2}";
                    repo.localStartIndex = 1;
                    repo.localLength = 5;
                }

                string tmp = remoteUrl.Substring(0, remoteUrl.LastIndexOf("/"));
                repo.sourceRepo.Owner = tmp.Substring(tmp.LastIndexOf("/") + 1);

                if (!ret.ContainsKey(repoName)) ret.Add(repoName, repo);
            }

            //For repositories that don't appear in the json file but have specail naming rules for juding whether localization or not.
            if (!ret.ContainsKey("PnP-Guidance"))
            {
                ret.Add("PnP-Guidance", new LocRepoFromAmbientConfiguration()
                {
                    repoNameRegex = "PnP-Guidance_[a-z]{2}-[a-z]{2}",
                    localeRegex = "_[a-z]{2}-[a-z]{2}",
                    localStartIndex = 1,
                    localLength = 5
                });
            }
            if (!ret.ContainsKey("O365API"))
            {
                ret.Add("O365API", new LocRepoFromAmbientConfiguration()
                {
                    repoNameRegex = "O365API_[a-z]{2}-[a-z]{2}",
                    localeRegex = "_[a-z]{2}-[a-z]{2}",
                    localStartIndex = 1,
                    localLength = 5
                });
            }
            if (!ret.ContainsKey("CommunityDocs"))
            {
                ret.Add("CommunityDocs", new LocRepoFromAmbientConfiguration()
                {
                    repoNameRegex = "CommunityDocs-[a-z]{4}",
                    localeRegex = "-[a-z]{4}",
                    localStartIndex = 1,
                    localLength = 4
                });
            }
            return ret;
        }

        public static string ComposeLocalizationRepoName(string repoName, string locale, Dictionary<string, LocRepoFromAmbientConfiguration> format)
        {
            LocRepoFromAmbientConfiguration value = format[repoName];
            if (value.targetRepo == null) return null;

            LocRepo targetRepo = value.targetRepo;
            if (string.IsNullOrEmpty(targetRepo.Name)) return repoName + "." + locale;
            else
            {
                locale = locale.Replace("-", "");
                return targetRepo.Name.Replace("<target_locale_without_dash>", locale);
            }
        }

        public static string[] GetLocalizationList(string repoName, string branch, string token, Dictionary<string, LocRepoFromAmbientConfiguration> format)
        {
            LocRepoFromAmbientConfiguration value = format[repoName];
            if (value.sourceRepo == null || value.handbackRepo == null) return null;
            LocRepo sourceRepo = value.sourceRepo;
            LocRepo handbackRepo = value.handbackRepo;

            //root folder of source repo
            string localConfigUrl = "https://raw.githubusercontent.com/" + sourceRepo.Owner + "/" + repoName + "/" + branch + "/.localization-config";
            
            JavaScriptSerializer j = new JavaScriptSerializer();
            Regex reg = new Regex("\"[a-z]{2}-[a-z]{2}\"");
            if (repoName.Equals("smbmadeira-content-pr", StringComparison.OrdinalIgnoreCase)) reg = new Regex("[a-z]{2}-[A-Z]{2}");

            try
            {
                string jsonContent = Util.GetJsonContent(localConfigUrl, token);
                //Many errors in json files of different repos, here we directly parser the string.
                MatchCollection mc = reg.Matches(jsonContent);
                string[] ret = new string[mc.Count];
                for (int i = 0; i < ret.Length; i++) ret[i] = mc[i].Value.Substring(1, 5);
                return ret.Distinct().ToArray();
            }
            catch (Exception ex)
            {
                if (ex.Message.Contains("404"))
                {
                    try
                    {
                        //ol-config/<source-repo-owner>/<source-repo-name>/<source-repo-branch>/.localization-config
                        localConfigUrl = "https://raw.githubusercontent.com/" + handbackRepo.Owner + "/" + handbackRepo.Name + "/master/ol-config/"
                            + sourceRepo.Owner + "/" + repoName + "/" + branch + "/.localization-config";

                        string jsonContent = Util.GetJsonContent(localConfigUrl, token);
                        //Many errors in json files of different repos, here we directly parser the string.
                        MatchCollection mc = reg.Matches(jsonContent);
                        string[] ret = new string[mc.Count];
                        for (int i = 0; i < ret.Length; i++)
                        {
                            if (repoName.Equals("smbmadeira-content-pr", StringComparison.OrdinalIgnoreCase)) ret[i] = mc[i].Value.ToLower();
                            else ret[i] = mc[i].Value.Substring(1, 5);
                        }
                        return ret.Distinct().ToArray();
                    }
                    catch (Exception)
                    {
                        return null;
                    }
                }
                throw ex;
            }
        }

        public static string[] GetLocalizationInfo(string repoName, Dictionary<string, LocRepoFromAmbientConfiguration> format)
        {
            string[] ret = new string[2];
            string repository = "", locale = "";

            //special case of wrong spelling
            if (repoName.Equals("azure-content-zhcn-pb"))
            {
                repository = "azure-content-pr";
                locale = "zhcn";
                ret[0] = repository;
                ret[1] = locale;
                return ret;
            }

            bool flag = false;
            foreach (KeyValuePair<string, LocRepoFromAmbientConfiguration> item in format)
            {
                repository = item.Key;
                LocRepoFromAmbientConfiguration value = item.Value;
                if (value != null && !string.IsNullOrEmpty(value.repoNameRegex) && !string.IsNullOrEmpty(value.localeRegex) && value.localStartIndex >= 0 && value.localLength > 0)
                {
                    Regex repoNameReg = new Regex(value.repoNameRegex);
                    if (repoName.Equals(repository, StringComparison.OrdinalIgnoreCase) || repoNameReg.IsMatch(repoName))
                    {
                        flag = true;
                        if (repoName.Equals(repository, StringComparison.OrdinalIgnoreCase)) locale = "en-us";
                        else
                        {
                            Regex localeReg = new Regex(value.localeRegex);
                            locale = localeReg.Match(repoName).Groups[0].Value.Substring(value.localStartIndex, value.localLength);
                        }
                        break;
                    }
                }
            }
            //For repos not appearing in the ambient configuration file, we use dot to judge whether localization or not. 
            if (!flag)
            {
                if (new Regex("[\\S]*\\.[a-z]{2}-[a-z]{2}").IsMatch(repoName))
                {
                    locale = repoName.ToLower().Substring(repoName.LastIndexOf('.') + 1);
                    repository = repoName.Substring(0, repoName.LastIndexOf('.'));
                }
                else
                {
                    locale = "en-us";
                    repository = repoName;
                }
            }
            ret[0] = repository;
            ret[1] = locale;
            return ret;
        }

        public static string GetGithubToken(string repositoryName)
        {
            ConfigManager configManager = new ConfigManager();

            string BuildStorageConnectionString = configManager.GetConfig("BackendJobs", "OPSBuildStorageConnectionString");

            var StorageAccount = CloudStorageAccount.Parse(BuildStorageConnectionString);
            var TableClient = StorageAccount.CreateCloudTableClient();
            var userTable = TableClient.GetTableReference("UserTableEntity");
            var repoTable = TableClient.GetTableReference("RepositoryTableEntity");

            TableQuery<RepositoryTableEntity> query = new TableQuery<RepositoryTableEntity>().Where(TableQuery.GenerateFilterCondition("GitRepositoryName", QueryComparisons.Equal, repositoryName)); ;
            RepositoryTableEntity repo = repoTable.ExecuteQuery(query).FirstOrDefault();

            TableQuery<UserTableEntity> userQuery = new TableQuery<UserTableEntity>().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, repo.CreatedBy));
            UserTableEntity user = userTable.ExecuteQuery(userQuery).FirstOrDefault();
            if (user == null) return null;
            return user.AccessToken;
        }
    }
}
