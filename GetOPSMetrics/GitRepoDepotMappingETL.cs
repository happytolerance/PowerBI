using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Insight.BackendJobs.GetOPSMetrics
{
    public class GitRepoDepotMappingETL : AbstractETL
    {
        protected override object Extract()
        {
            List<GitRepoDHSDepotMap> ret = new List<GitRepoDHSDepotMap>();
            string BuildStorageConnectionString = configManager.GetConfig("BackendJobs", "OPSBuildStorageConnectionString");
            var StorageAccount = CloudStorageAccount.Parse(BuildStorageConnectionString);
            var TableClient = StorageAccount.CreateCloudTableClient();
            var docsetTable = TableClient.GetTableReference("DocsetTableEntity");
            TableQuery<DocsetTableEntity> query = new TableQuery<DocsetTableEntity>();
            foreach (DocsetTableEntity docset in docsetTable.ExecuteQuery(query))
            {
                //Ignore those IsPageOver = True
                if(docset.IsPaveOver)
                {
                    continue;
                }
                
                string prodName = "";
                if (!string.IsNullOrEmpty(docset.ProductName) && !string.Equals(docset.ProductName, "NULL", StringComparison.OrdinalIgnoreCase))
                {
                    prodName = docset.ProductName + ".";
                }

                ret.Add(new GitRepoDHSDepotMap()
                {
                    GitRepoId = docset.PartitionKey,
                    DHSDepotName = prodName + docset.DocsetName
                });

                // For compatibility for Pre Sprint 94, when ProdName is not prefix-ed before docset name
                if (!string.IsNullOrEmpty(prodName))
                {
                    ret.Add(new GitRepoDHSDepotMap()
                    {
                        GitRepoId = docset.PartitionKey,
                        DHSDepotName = docset.DocsetName
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
            List<GitRepoDHSDepotMap> maps = obj as List<GitRepoDHSDepotMap>;

            using (DataTable dt = new DataTable())
            {
                dt.Columns.Add("RepositoryId");
                dt.Columns.Add("DHSDepotName");

                foreach (GitRepoDHSDepotMap map in maps)
                {
                    dt.Rows.Add(map.GitRepoId, map.DHSDepotName);
                }

                Dictionary<string, DataTable> paramDic = new Dictionary<string, DataTable>();
                paramDic.Add("GitRepoDHSDepotMapTableParam?GitRepoDHSDepotMapType", dt);

                string OPSDataSyncConnStr = configManager.GetConfig("DBConnectionString", "OPSDataSync");
                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
                InsightDBHelper.InsightDBHelper.ExecuteSP("SP_GitRepoDHSDepotMap", paramDic, true);
            }

            return;
        }
    }
}
