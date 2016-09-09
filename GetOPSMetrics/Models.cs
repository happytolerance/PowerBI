using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Runtime.Serialization;


namespace Insight.BackendJobs.GetOPSMetrics
{
    class GitHubRepository
    {
        public string PartitionKey;
        public string Owner;
        public string RepositoryName;
        public string RepositoryUrl;
        public string AuthToken;
        public string Timestamp;
        public List<GitBranch> Branches;
        public string GitRepositoryType;
        public bool IsLocalization = true;
    }

    class GitRepoDHSDepotMap
    {
        public string GitRepoId;
        public string DHSDepotName;
    }

    class GitCommitValueAgg
    {
        public int CommitCount;
        public int CommitMergedCount;
        public string LatestCommitDateTime;
    }

    class GitRepoTopicInfo_Detail
    {
        public string PartitionKey;
        public string BranchName;
        public List<string> Topics;
    }

    class GitRepoRecentUpdatedTopicCountInfo
    {
        public string PartitionKey;
        public int RecentUpdatedTopicCount;
    }

    class GitRepoTopicLocalizationStatus
    {
        public string PartitionKey;
        public string Locale;
        public string Branch;
        public string TopicPath;
        public DateTime? HandoffDateTime;
        public DateTime? HandbackDateTime;
    }

    class GitRepoTopicPublishRecord
    {
        public string PartitionKey;
        public int PullRequestNumber;
        public string TopicPath;
        public DateTime PublishDateTime;
        public HashSet<string> AuthorIds;
    }

    class GitRepoTopicPublishRecord2
    {
        public string PublishId;
        public string PartitionKey;
        public List<GitRepoTopicPublishContentUpdate> TopicPaths;
        public string Branch;
        public DateTime? StartTime;
        public DateTime? EndTime;
        //public HashSet<string> AuthorIds;
        public HashSet<string> AuthorLogins;
        public HashSet<string> AuthorNames;
        public string Status;
    }

    class GitRepoTopicPublishContentUpdate
    {
        public string TopicPath;
        public int Insertions;
        public int Deletions;
        public int? InsertionsOfWord;
        public int? DeletionsOfWord;
        public string Status;
    }

    class RepositoryTableEntity : TableEntity
    {
        public string GitRepositoryUrl { get; set; }
        public string GitRepositoryAccount { get; set; }
        public string GitRepositoryName { get; set; }
        public string CreatedBy { get; set; }
        public string GitRepositoryType { get; set; }
    }

    class DocsetTableEntity : TableEntity
    {
        public string DocsetName { get; set; }
        public string ProductName { get; set; }
        public bool IsPaveOver { get; set; }
    }

    class UserTableEntity : TableEntity
    {
        public string AccessToken { get; set; }
    }

    [DataContract]
    public class GitBranch
    {
        [DataMember(Name = "name")]
        public string Name;
    }

    [DataContract]
    public class GitContributor
    {
        [DataMember(Name = "login")]
        public string Login;

        [DataMember(Name = "type")]
        public string Type;

        [DataMember(Name = "contributions")]
        public int Contributions;

        [DataMember(Name = "id")]
        public string Id;

        public string GitRepoId;
    }

    [DataContract]
    public class GitCommit
    {
        [DataMember(Name = "commit")]
        public GitInnerCommit Commit;
    }

    [DataContract]
    public class GitCommitNOInPull
    {
        [DataMember(Name = "commits")]
        public int Commits;
    }

    [DataContract]
    public class GitInnerCommit
    {
        [DataMember(Name = "message")]
        public string Message;

        [DataMember(Name = "committer")]
        public GitCommitter Committer;
    }

    [DataContract]
    public class GitCommitter
    {
        [DataMember(Name = "date")]
        public string Date;
    }

    [DataContract]
    public class GitUser
    {
        [DataMember(Name = "location")]
        public string Location;

        [DataMember(Name = "id")]
        public string ID;

        [DataMember(Name = "created_at")]
        public string Created_At;

        [DataMember(Name = "updated_at")]
        public string Updated_At;

        [DataMember(Name = "company")]
        public string Company;

        [DataMember(Name = "name")]
        public string Name;

        [DataMember(Name = "email")]
        public string Email;

        public string Login;

        public string MSType;
    }

    [DataContract]
    public class GitVSOUser
    {
        [DataMember(Name = "id")]
        public string ID;

        [DataMember(Name = "displayName")]
        public string DisplayName;

        [DataMember(Name = "uniqueName")]
        public string UniqueName;
    }

    public enum MSTypeEnum
    {
        Content_Team,
        Product_Team,
        Unknown,
        UnChecked
    }

    [DataContract]
    public class GitPull
    {
        [DataMember(Name = "number")]
        public int Number;

        [DataMember(Name = "state")]
        public string State;

        [DataMember(Name = "created_at")]
        public DateTime? Created_At;

        [DataMember(Name = "updated_at")]
        public DateTime? Updated_At;

        [DataMember(Name = "closed_at")]
        public DateTime? Closed_At;

        [DataMember(Name = "merged_at")]
        public DateTime? Merged_At;

        [DataMember(Name = "user")]
        public GitContributor User;

        [DataMember(Name = "base")]
        public GitPullDirection Base;

        [DataMember(Name = "head")]
        public GitPullDirection Head;

        public string GitRepoId;

        public int CommitsCount = 0;
    }

    [DataContract]
    public class GitPullDirection
    {
        [DataMember(Name = "ref")]
        public string Ref;
    }

    public class AzureHDInsightConnInfo
    {
        public string storageAccountName;
        public string storageAccountKey;
        public string storageContainerName;
        public string subscriptionID;
        public string clusterName;
        public string thumbprint;
    }

    [DataContract]
    public class OPSBuildInfo
    {
        [DataMember(Name = "id")]
        public string id;

        [DataMember(Name = "branch_name")]
        public string branch_name;

        [DataMember(Name = "build_type")]
        public string build_type;

        [DataMember(Name = "change_log_url")]
        public string change_log_url;

        [DataMember(Name = "commit_id")]
        public string commit_id;

        [DataMember(Name = "commit_message")]
        public string commit_message;

        [DataMember(Name = "created_by")]
        public string created_by;

        [DataMember(Name = "started_at")]
        public string started_at;

        [DataMember(Name = "ended_at")]
        public string ended_at;

        [DataMember(Name = "repository_id")]
        public string repository_id;

        [DataMember(Name = "repository_account")]
        public string repository_account;

        [DataMember(Name = "repository_name")]
        public string repository_name;

        [DataMember(Name = "status")]
        public string status;
    }

    [DataContract]
    public class OPSBuildInfo_ChangeLog_Commit
    {
        [DataMember(Name = "author_name")]
        public string author_name;

        [DataMember(Name = "author_login_name")]
        public string author_login_name;

        [DataMember(Name = "commit_date")]
        public string commit_date;

        [DataMember(Name = "commit_sha")]
        public string commit_sha;

        [DataMember(Name = "committer_name")]
        public string committer_name;

        [DataMember(Name = "committer_login_name")]
        public string committer_login_name;

        [DataMember(Name = "message")]
        public string message;
    }

    [DataContract]
    public class OPSBuildInfo_ChangeLog_File
    {
        [DataMember(Name = "deletions")]
        public int deletions;

        [DataMember(Name = "insertions")]
        public int insertions;

        [DataMember(Name = "file_name")]
        public string file_name;

        [DataMember(Name = "status")]
        public string status;
    }

    [DataContract]
    public class OPSBuildInfo_ChangeLog
    {
        [DataMember(Name = "commits")]
        public List<OPSBuildInfo_ChangeLog_Commit> commits;

        [DataMember(Name = "files")]
        public List<OPSBuildInfo_ChangeLog_File> files;

        [DataMember(Name = "redirect_url")]
        public string redirect_url;
    }

    [DataContract]
    public class FilePublishInfo
    {
        [DataMember(Name = "deletions")]
        public int deletions;

        [DataMember(Name = "insertions")]
        public int insertions;

        [DataMember(Name = "changes")]
        public int changes;

        [DataMember(Name = "filename")]
        public string filename;

        [DataMember(Name = "status")]
        public string status;

        [DataMember(Name = "patch")]
        public string patch;
    }

    public class FilePublishInfoList
    {
        [DataMember(Name = "files")]
        public List<FilePublishInfo> files;
    }

    [DataContract]
    public class GitVSORepositoryList
    {
        [DataMember(Name = "count")]
        public int Count;

        [DataMember(Name = "value")]
        public List<GitVSORepository> Value;
    }

    [DataContract]
    public class GitVSORepository
    {
        [DataMember(Name = "id")]
        public string Id;

        [DataMember(Name = "name")]
        public string Name;

        [DataMember(Name = "remoteUrl")]
        public string RemoteUrl;
    }

    [DataContract]
    public class GitVSOPullList
    {
        [DataMember(Name = "count")]
        public int Count;

        [DataMember(Name = "value")]
        public List<GitVSOPull> Value;
    }

    [DataContract]
    public class GitVSOPull
    {
        [DataMember(Name = "pullRequestId")]
        public int pullRequestId;

        [DataMember(Name = "status")]
        public string Status;

        [DataMember(Name = "mergeStatus")]
        public string MergeStatus;

        [DataMember(Name = "creationDate")]
        public DateTime? CreationDate;

        [DataMember(Name = "closedDate")]
        public DateTime? ClosedDate;

        [DataMember(Name = "createdBy")]
        public GitVSOUser CreatedBy;

        [DataMember(Name = "sourceRefName")]
        public string SourceRefName;

        [DataMember(Name = "targetRefName")]
        public string TargetRefName;

        public string GitRepoId;

        public int CommitsCount = 0;
    }

    [DataContract]
    public class RepoListFromAmbientConfiguration
    {
        [DataMember(Name = "repos")]
        public List<LocRepoFromAmbientConfiguration> Repos;
    }

    [DataContract]
    public class LocRepoFromAmbientConfiguration
    {
        [DataMember(Name = "sourceRepo")]
        public LocRepo sourceRepo;

        [DataMember(Name = "targetRepo")]
        public LocRepo targetRepo;

        [DataMember(Name = "handbackRepo")]
        public LocRepo handbackRepo;

        public string localeRegex;

        public string repoNameRegex;

        public int localLength = 0;

        public int localStartIndex = -1;
    }

    [DataContract]
    public class LocRepo
    {
        [DataMember(Name = "remoteUrl")]
        public string RemoteUrl;
        [DataMember(Name = "owner")]
        public string Owner;
        [DataMember(Name = "name")]
        public string Name;
        [DataMember(Name = "workingBranch")]
        public string WorkingBranch;
    }
}
