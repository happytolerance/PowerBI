using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.IO;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

using Insight.BackendJobs.InsightDBHelper;

namespace Insight.BackendJobs.GetOPSMetrics
{
    public class OPSPublishETL : AbstractETL
    {
        protected override object Extract()
        {
            string OPSDataSyncConnStr = configManager.GetConfig("DBConnectionString", "OPSDataSync");
            string hdInsightConn = configManager.GetConfig("AzureHDInsight", "ConnectionInfo");
            DataContractJsonSerializer jsonSerializer = new DataContractJsonSerializer(typeof(AzureHDInsightConnInfo));
            AzureHDInsightConnInfo azureHDInsightConnInfo =
                jsonSerializer.ReadObject(new MemoryStream(Encoding.UTF8.GetBytes(hdInsightConn))) as AzureHDInsightConnInfo;

            InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(OPSDataSyncConnStr);
            DataRow[] dataRow = InsightDBHelper.InsightDBHelper.ExecuteQuery("select top 1 LatestRecordDate from [OPS_Publish_LatestRecordDate] with (nolock)", true);
            DateTime end = DateTime.UtcNow.AddDays(-1);
            DateTime start = end;
            if (!(dataRow == null
                        || dataRow.Length == 0
                        || dataRow[0].ItemArray == null
                        || dataRow[0].ItemArray.Length == 0
                        || dataRow[0].ItemArray[0] == null
                        || string.IsNullOrEmpty(dataRow[0].ItemArray[0].ToString())
                        || string.Equals(dataRow[0].ItemArray[0].ToString(), "NULL", StringComparison.OrdinalIgnoreCase))
                        )
            {
                start = DateTime.Parse(dataRow[0].ItemArray[0].ToString()).AddDays(1);
            }

            string dateStrStart1 = start.ToString("yyyy-MM-dd") + " 00:00:00";
            string dateStrEnd1 = end.ToString("yyyy-MM-dd") + " 23:59:59";
            string dateStrStart2 = start.ToString("yyyy-MM-dd") + "-00";
            string dateStrEnd2 = end.ToString("yyyy-MM-dd") + "-23";
            string hql = string.Format(
                            @"SELECT LogEventTime, ApiInput, ApiOutput, Duration, ResultCode FROM CapsApiTrace 
                            WHERE Environment = 'PROD' AND TraceEnv = 'prod' AND ApiName = 'ProcessMessage\\.Publish' 
                            AND ComponentName = 'Microsoft\\.OpenPublishing\\.Build\\.Backend' 
                            AND cast(LogEventTime as bigint) >= UNIX_TIMESTAMP('{0}') AND cast(LogEventTime as bigint) <= UNIX_TIMESTAMP('{1}') 
                            AND LogTime between '{2}' and '{3}' LIMIT 20000;",
                            dateStrStart1, dateStrEnd1, dateStrStart2, dateStrEnd2);

            //return new string[] { 
            //    end.ToString("yyyy-MM-dd"), 
            //    Util.RunHiveQuery(
            //        hql, 
            //        end.ToString("yyyy-MM-dd"),
            //        azureHDInsightConnInfo.subscriptionID,
            //        azureHDInsightConnInfo.thumbprint,
            //        azureHDInsightConnInfo.clusterName,
            //        azureHDInsightConnInfo.storageAccountName,
            //        azureHDInsightConnInfo.storageAccountKey,
            //        azureHDInsightConnInfo.storageContainerName
            //        ) 
            //};

            // This method is obsolete.
            return null;
        }

        protected override object Transform(object obj)
        {
            return obj;
        }

        protected override void Load(object obj)
        {
            string[] data = obj as string[];
            string queryResult = data[1];

#if DEBUG
            //queryResult = "2016-12-01 08:52:50.208\t\\{\"message\":\"MessageId:\\ RepoId:\\ e3e069e5-e282-c4c2-b0f4-64ede64992e5,\\ BuildId:\\ 201512010145570116-master,\\ BuildType:\\ Commit,\\ Action:\\ Publish,\\ Category:\\ ,\\ Priority:\\ Default,\\ SubMessageIndex:\\ 0\"}\t\\{\"StatusCode\":\"EngineError\",\"Op_ErrorMessage\":\"File\\ share\\ access\\ exception\\ of\\ path\\ \\\\u0027\\\\\\\\\\\\\\\\opbuildstorageprod\\.file\\.core\\.windows\\.net\\\\\\\\e3e069e5-17190aeb\\\\u0027,\\ error\\ code\\ \\\\u00271312\\\\u0027,\\ detailed\\ message:\\ Cannot\\ mount\\ remote\\ path\\ \\\\u0027\\\\\\\\\\\\\\\\opbuildstorageprod\\.file\\.core\\.windows\\.net\\\\\\\\e3e069e5-17190aeb\\\\u0027\\ to\\ local\\ path\\ \\\\u0027\\\\\\\\\\\\\\\\opbuildstorageprod\\.file\\.core\\.windows\\.net\\\\\\\\e3e069e5-17190aeb\\\\u0027\\.\\ Current\\ identity:\\ NT\\ AUTHORITY\\\\\\\\SYSTEM\\.\",\"Op_ErrorDetail\":\"Microsoft\\.OpenPublishing\\.Build\\.DataAccessor\\.Interface\\.FileShareAccessException:\\ File\\ share\\ access\\ exception\\ of\\ path\\ \\\\u0027\\\\\\\\\\\\\\\\opbuildstorageprod\\.file\\.core\\.windows\\.net\\\\\\\\e3e069e5-17190aeb\\\\u0027,\\ error\\ code\\ \\\\u00271312\\\\u0027,\\ detailed\\ message:\\ Cannot\\ mount\\ remote\\ path\\ \\\\u0027\\\\\\\\\\\\\\\\opbuildstorageprod\\.file\\.core\\.windows\\.net\\\\\\\\e3e069e5-17190aeb\\\\u0027\\ to\\ local\\ path\\ \\\\u0027\\\\\\\\\\\\\\\\opbuildstorageprod\\.file\\.core\\.windows\\.net\\\\\\\\e3e069e5-17190aeb\\\\u0027\\.\\ Current\\ identity:\\ NT\\ AUTHORITY\\\\\\\\SYSTEM\\.\\\\r\\\\n\\ \\ \\ at\\ Microsoft\\.OpenPublishing\\.Build\\.DataAccessor\\.Azure\\.FileShareAccessor\\.HandleNetworkMountException\\(Action\\ action,\\ String\\ path\\)\\ in\\ C:\\\\\\\\agent\\\\\\\\_work\\\\\\\\e39e67c4\\\\\\\\s\\\\\\\\Libraries\\\\\\\\DataAccessor\\.Azure\\\\\\\\File\\\\\\\\FileShareAccessor\\.cs:line\\ 95\\\\r\\\\n\\ \\ \\ at\\ Microsoft\\.OpenPublishing\\.Build\\.DataAccessor\\.Azure\\.FileShareAccessor\\.Open\\(String\\ shareName\\)\\ in\\ C:\\\\\\\\agent\\\\\\\\_work\\\\\\\\e39e67c4\\\\\\\\s\\\\\\\\Libraries\\\\\\\\DataAccessor\\.Azure\\\\\\\\File\\\\\\\\FileShareAccessor\\.cs:line\\ 51\\\\r\\\\n\\ \\ \\ at\\ Microsoft\\.OpenPublishing\\.Build\\.FileShareAccessorPerf\\.Open\\(String\\ shareName\\)\\ in\\ C:\\\\\\\\agent\\\\\\\\_work\\\\\\\\e39e67c4\\\\\\\\s\\\\\\\\Libraries\\\\\\\\DataAccessorUtility\\\\\\\\PerfDecorator\\\\\\\\FileShareAccessorPerf\\.cs:line\\ 27\\\\r\"}\t5911\t500\r\n";
#endif

            string OPSDataSyncConnStr = configManager.GetConfig("DBConnectionString", "OPSDataSync");
            Export(OPSDataSyncConnStr, queryResult, data[0]);

        }

        private static void FillExportMappingColumns(SqlBulkCopy cp, DataTable dt)
        {
            cp.ColumnMappings.Add(0, "RepositoryId");
            cp.ColumnMappings.Add(1, "LogEventTime");
            cp.ColumnMappings.Add(2, "Branch");
            cp.ColumnMappings.Add(3, "Duration");
            cp.ColumnMappings.Add(4, "ResultCode");
            cp.ColumnMappings.Add(5, "Error");

            dt.Columns.Add("RepositoryId", typeof(string));
            dt.Columns.Add("LogEventTime", typeof(string));
            dt.Columns.Add("Branch", typeof(string));
            dt.Columns.Add("Duration", typeof(int));
            dt.Columns.Add("ResultCode", typeof(int));
            dt.Columns.Add("Error", typeof(string));
        }

        private void Export(string connStr, string queryResult, string runDate)
        {
            List<string> lines = queryResult.Split(new char[] { '\n' }, StringSplitOptions.RemoveEmptyEntries).ToList();

            int retryLimit = 2;
            int retryCnt = 0;
            while (retryCnt < retryLimit)
            {
                try
                {
                    using (SqlConnection conn = new SqlConnection(connStr))
                    {
                        using (SqlBulkCopy cp = new SqlBulkCopy(conn))
                        {
                            cp.DestinationTableName = "OPS_Publish";
                            cp.BulkCopyTimeout = 3600; /*3600 * 5;*/
                            conn.Open();

                            using (DataTable dt = new DataTable())
                            {
                                FillExportMappingColumns(cp, dt);
                                List<string[]> rows = lines.Select(x => x.Split('\x09')).ToList();
                                rows.ForEach(row =>
                                {
                                    if (row.Length == 5)
                                    {
                                        //LogEventTime, ApiInput, ApiOutput, Duration, ResultCode FROM CapsApiTrace 
                                        // api input sample:
                                        // {"message":"MessageId: RepoId: e3e069e5-e282-c4c2-b0f4-64ede64992e5, BuildId: 201511302231031308-master, BuildType:...
                                        string[] apiInputs = Regex.Unescape(row[1]).Split(':');
                                        string repoId = apiInputs[3].Trim().Substring(0, 36);
                                        string rawBranch = apiInputs[4];
                                        int branchStart = rawBranch.IndexOf('-') + 1;
                                        int branchEnd = rawBranch.IndexOf(',');
                                        string branchName = rawBranch.Substring(branchStart, branchEnd - branchStart);
                                        string[] newRow = new string[]
                                        {
                                            repoId,
                                            row[0],
                                            branchName,
                                            row[3],
                                            row[4].TrimEnd('\r'),
                                            int.Parse(row[4].TrimEnd('\r')) < 500 ? "" : Regex.Unescape(row[2])
                                        };

                                        dt.Rows.Add(newRow);
                                    }
                                });

                                cp.WriteToServer(dt);

                                InsightDBHelper.InsightDBHelper.ConnectDBWithConnectString(connStr);
                                InsightDBHelper.InsightDBHelper.ExecuteNonQuery(string.Format("update [OPS_Publish_LatestRecordDate] set LatestRecordDate = '{0}'", runDate), true);
                            }
                        }
                    }

                    break;
                }
                catch (SqlException sqlEx)
                {
                    retryCnt++;
                    Thread.Sleep(20000);
                    if (retryCnt >= retryLimit)
                    {
                        throw sqlEx;
                    }
                }
            }
        }
    }
}
