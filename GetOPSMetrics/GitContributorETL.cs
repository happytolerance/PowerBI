using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Net;
using System.Linq;
using System.Web.Script.Serialization;

namespace Insight.BackendJobs.GetOPSMetrics
{
    public class GitContributorETL : AbstractETL
    {
        protected override object Extract()
        {
            if (SharedObject_Prod_GitHub == null)
            {
                return null;
            }

            //[UserList, GitRepo-Contributor-mapping]
            List<object> ret = new List<object>();

            List<GitHubRepository> repos = SharedObject_Prod_GitHub as List<GitHubRepository>;
            HashSet<string> usersDone = new HashSet<string>();
            List<GitUser> userList = new List<GitUser>();
            List<GitContributor> contributorList = new List<GitContributor>();

            HashSet<string> contributorHS = new HashSet<string>();

            foreach (GitHubRepository repo in repos)
            {
                List<GitContributor> contributors = 
                    Util.CallGitHubAPI<List<GitContributor>>(string.Format("https://api.github.com/repos/{0}/{1}/contributors", repo.Owner, repo.RepositoryName), repo.AuthToken) as List<GitContributor>;

                if (contributors == null)
                {
                    continue;
                }

                foreach(var contributor in contributors)
                {
                    contributor.GitRepoId = repo.PartitionKey;
                    
                    if(usersDone.Contains(contributor.Login))
                    {
                        continue;
                    }

                    GitUser gitUser = Util.CallGitHubAPI<GitUser>(string.Format("https://api.github.com/users/{0}", contributor.Login), repo.AuthToken) as GitUser;
                    if(gitUser == null)
                    {
                        continue;
                    }

                    usersDone.Add(contributor.Login);

                    gitUser.Login = contributor.Login;
                    userList.Add(gitUser);

                    string tmpContributorKey = contributor.GitRepoId + "_" + contributor.Id;
                    if(!contributorHS.Contains(tmpContributorKey))
                    {
                        contributorList.Add(contributor);
                    }

                    contributorHS.Add(tmpContributorKey);
                }
            }

            ret.Add(userList);
            ret.Add(contributorList);
            return ret;
        }

        protected override object Transform(object obj)
        {
            return obj;
        }

        protected override void Load(object obj)
        {
            if(obj == null)
            {
                return;
            }

            List<object> objList = obj as List<object>;
            List<GitUser> users = objList[0] as List<GitUser>;
            List<GitContributor> contributors = objList[1] as List<GitContributor>;

            using (DataTable dt = new DataTable(), dt2 = new DataTable())
            {
                dt.Columns.Add("GitHubId");
                dt.Columns.Add("Login");
                dt.Columns.Add("IsMS");
                dt.Columns.Add("CreatedAt");
                dt.Columns.Add("UpdatedAt");
                dt.Columns.Add("Name");
                dt.Columns.Add("Email");
                dt.Columns.Add("Company");
                dt.Columns.Add("Location");
                dt.Columns.Add("MSType");
                foreach (GitUser user in users)
                {
                    bool isMS = IsUserMS(user);
                    string msType = GetUserMSType(user, isMS);

                    dt.Rows.Add(
                        user.ID,
                        user.Login,
                        isMS,
                        user.Created_At,
                        user.Updated_At,
                        user.Name,
                        user.Email,
                        user.Company,
                        user.Location,
                        msType
                        );
                }

                dt2.Columns.Add("RepositoryId");
                dt2.Columns.Add("GitHubId");
                dt2.Columns.Add("Contributions");
                dt2.Columns.Add("TableLastUpdatedAt");
                foreach (GitContributor contributor in contributors)
                {
                    dt2.Rows.Add(
                        contributor.GitRepoId,
                        contributor.Id,
                        contributor.Contributions,
                        DateTime.UtcNow.ToString("yyyy-MM-dd")
                        );
                }

                Dictionary<string, DataTable> paramDic = new Dictionary<string, DataTable>();
                paramDic.Add("GitUserTableParam?GitUserType", dt);
                paramDic.Add("GitContributorTableParam?GitContributorType", dt2);

                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_OPSStoreGitUsersAndContributors", paramDic, true);
            }

            return;
        }

        private bool IsUserMS(GitUser user)
        {
            if(user == null)
            {
                return false;
            }

            if(!string.IsNullOrEmpty(user.Company) &&
                (
                    string.Equals(user.Company.Trim(), "Microsoft", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(user.Company.Trim().Replace(" ", ""), "MicrosoftCorporation", StringComparison.OrdinalIgnoreCase)
                    || string.Equals(user.Company.Trim().Replace(".", ""), "Microsoft", StringComparison.OrdinalIgnoreCase)
                )
              )
            {
                return true;
            }

            if (!string.IsNullOrEmpty(user.Email) && user.Email.Contains("@microsoft.com"))
            {
                return true;
            }

            return false;
        }

        private string GetUserMSType(GitUser user, bool isMS)
        {
            if(!isMS)
            {
                return null;
            }

            // TODO
            return MSTypeEnum.UnChecked.ToString().Replace("_", " ");
        }
    }
}
