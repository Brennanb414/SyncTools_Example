using System;
using System.Configuration;
using System.Data.SqlClient;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Xml;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;


//this program requires sp_generate_merge available to the user account on SQL Server
//(available here: https://github.com/readyroll/generate-sql-merge)
namespace SyncTools
{
    class syncTools
    {
        //Method for trying a sql query on target connection and catching sql errors, but ultimately proceeding
        public void TrySqlStatement(string sql, SqlConnection connection , TraceWriter log)
        {
            SqlCommand cmd = new SqlCommand(sql, connection);
            try
            {
                cmd.ExecuteNonQuery();
            }
            catch (System.Data.SqlClient.SqlException ex)
            {
                log.Info(ex.ToString());
            }
        }

        //simplify a fully qualified sql server table name: [dbo].[table1] to dbo_table1
        //for use in indexes
        private string SimplifyTableName(string fullTableName)
        {
            string simpleTableName = fullTableName.Replace('.', '_').Replace("[", null).Replace("]", null);
            return simpleTableName;
        }

        //create a primary key on a table as required by the sp_generate_merge statement
        public void CreatePrimaryKey(string ColName, string fullTableName, SqlConnection connection)
        {
            
            string sql = string.Format(@"ALTER TABLE {0}
					ADD CONSTRAINT {1} PRIMARY KEY ({2})", fullTableName, String.Concat(string.Format("{0}Staging_", SimplifyTableName(fullTableName)), ColName, "_pk"), ColName);
            SqlCommand cmd = new SqlCommand(sql, connection);
            cmd.ExecuteNonQuery();
        }

        //get columns currently in a table
        public List<string> GetColumns(string fullTableName, SqlConnection connection)
        {
            string sql = string.Format("SELECT TOP 0 * FROM {0}", fullTableName);
            SqlCommand targetCommand = new SqlCommand(sql, connection);
            SqlDataReader targetDataReader = targetCommand.ExecuteReader();
            List<string> targetCols = new List<string>();
            for (int i = 0; i < targetDataReader.FieldCount; i++)
            {
                targetCols.Add(targetDataReader.GetName(i));
            }
            targetDataReader.Close();
            return targetCols;
        }

        //builds the select statement based on built column list
        public string BuildSelectStatement(List<string> sharedKeys, string fullTableName, string whereStatement = null, string sortColumn = null)
        {
            string selectStatement = "SELECT ";
            selectStatement += string.Format("\n{0}", sharedKeys[0]);
            for (int i = 1; i < sharedKeys.Count; i++)
            {
                selectStatement += string.Format(",\n{0}", sharedKeys[i]);
            }

            selectStatement += string.Format("\nFROM {0}", fullTableName);

            if (whereStatement != null)
            {
                selectStatement += string.Format("\nWHERE {0}", whereStatement);
            }

            if (sortColumn != null)
            {
                selectStatement += string.Format("\nORDER BY {0} ASC", sortColumn);
            }


            return selectStatement;

        }

        //merges the staging table to target table - currently doesn't work with temp tables because of sp_generate_merge (12/6/2018) (issue: https://github.com/readyroll/generate-sql-merge/issues/38)
        public void MergeStagingToTarget(SqlConnection connection, string targetSchema, string targetTable, string stagingTable, string fromStatement = "NULL")
        {


            string SP_String = string.Format(@"EXEC sp_generate_merge
			@table_name = '{0}', --The table / view for which the MERGE statement will be generated using the existing data
			@target_table = '{1}', --Use this parameter to specify a different table name into which the data will be inserted/ updated / deleted
			@from = {2}, --Use this parameter to filter the rows based on a filter condition(using WHERE)
			@include_timestamp = 0, --Specify 1 for this parameter, if you want to include the TIMESTAMP / ROWVERSION column's data in the MERGE statement
			@debug_mode = 0, --If @debug_mode is set to 1, the SQL statements constructed by this procedure will be printed for later examination
			@schema = '{3}', --Use this parameter if you are not the owner of the table
			@ommit_images = 0, --Use this parameter to generate MERGE statement by omitting the 'image' columns
			@ommit_identity = 0, --Use this parameter to ommit the identity columns
			@top = NULL, --Use this parameter to generate a MERGE statement only for the TOP n rows
			@cols_to_include = NULL, --List of columns to be included in the MERGE statement
			@cols_to_exclude = NULL, --List of columns to be excluded from the MERGE statement
			@update_only_if_changed = 1, --When 1, only performs an UPDATE operation if an included column in a matched row has changed.
			@delete_if_not_matched = 0, --When 1, deletes unmatched source rows from target, when 0 source rows will only be used to update existing rows or insert new.
			@disable_constraints = 0, --When 1, disables foreign key constraints and enables them after the MERGE statement
			@ommit_computed_cols = 0, --When 1, computed columns will not be included in the MERGE statement
			@include_use_db = 1, --When 1, includes a USE[DatabaseName] statement at the beginning of the generated batch
			@results_to_text = 0, --When 1, outputs results to grid/ messages window.When 0, outputs MERGE statement in an XML fragment.
			@include_rowsaffected = 0, --When 1, a section is added to the end of the batch which outputs rows affected by the MERGE
			@nologo = 1,
			@batch_separator = ''-- Batch separator to use", stagingTable, targetTable, fromStatement, targetSchema);

            SqlCommand targetCommand = new SqlCommand(SP_String, connection);
            XmlReader Reader = targetCommand.ExecuteXmlReader();
            while (Reader.Read())
            {
                targetCommand.CommandText = Reader.Value;
            }

            if (GetTableSize(string.Concat(targetSchema,'.',stagingTable), connection) > 0)
            {
                targetCommand.ExecuteNonQuery();
            }
        }

        //drop a table
        public void DropTable(string fullTableName, SqlConnection connection)
        {
            SqlCommand command = new SqlCommand(string.Format("DROP TABLE {0}", fullTableName), connection);
            command.ExecuteNonQuery();
        }

        //get a table's size
        public int GetTableSize(string fullTableName, SqlConnection connection)
        {
            string sizeStatement = string.Format("SELECT COUNT(*) FROM {0}", fullTableName);
            SqlCommand targetCommand = new SqlCommand(sizeStatement, connection);
            int stagingSize = (int)targetCommand.ExecuteScalar();
            return stagingSize;
        }

        //get the bottom merge record for delete/merge
        private string GetBotMergeRecord(int offset, string fullTableName, string sortColumn, SqlConnection connection)
        {
            //grab the (offset)th record, or 1
            string sqlStatement = string.Format("SELECT COALESCE((SELECT {1} FROM {0} ORDER BY {1} ASC OFFSET {2} ROWS FETCH NEXT 1 ROWS ONLY),(SELECT MIN({1}) FROM {0}),'1900-01-01')", fullTableName, sortColumn, (offset - 1));
            SqlCommand targetCommand = new SqlCommand(sqlStatement, connection);
            string botMergeRecord = targetCommand.ExecuteScalar().ToString();
            return botMergeRecord;

        }
        
        //deletes records from staging table if they are below the bottom merge record, if no bottom merge record then deletes all records
        private void DeleteRecordsFromTable(string fullTableName, string sortColumn, SqlConnection connection, TraceWriter log, string botMergeRecord = null)
        {
            string sqlStatement;
            SqlCommand targetCommand;
            if (botMergeRecord != null)
            {
                sqlStatement = string.Format("DELETE FROM {0} WHERE {1} < '{2}'", fullTableName, sortColumn, botMergeRecord);
                targetCommand = new SqlCommand(sqlStatement, connection);
            }
            else
            {
                sqlStatement = string.Format("DELETE FROM {0} ", fullTableName);
                targetCommand = new SqlCommand(sqlStatement, connection);
            }
            int rows_affected = targetCommand.ExecuteNonQuery();
            log.Info(string.Concat("Merged and Deleted ", rows_affected, " rows from Staging"));

        }

        //batch merge a whole staging table to a target table
        private void BatchedMerge(string targetSchema, string targetTable, string stagingTable, string sortColumn, SqlConnection connection,TraceWriter log)
        {
            string fullTargetTable = String.Concat(targetSchema, targetTable);
            string fullStagingTable = String.Concat(targetSchema, stagingTable);

            int stagingSize = GetTableSize(fullStagingTable, connection);
            int timesToMerge = (int)Math.Ceiling((decimal)stagingSize / 1000) + 1;
            string fromStatement;
            string bottomMerge;
            for (int i = 0; i < timesToMerge; i++)
            {
                fromStatement = string.Format("'FROM {0} WHERE ", fullStagingTable);
                bottomMerge = GetBotMergeRecord(1000, fullStagingTable, sortColumn, connection);
                fromStatement += String.Concat(sortColumn, " < ''", bottomMerge, "'''");

                //only run again if the staging table has anything in it
                stagingSize = GetTableSize(fullStagingTable, connection);
                log.Info($"{fullStagingTable} Table Size: {stagingSize.ToString()}");
                if (stagingSize > 0)
                {
                    //if staging table is > 1000, run with a conditional statement, otherwise run for everything
                    if (stagingSize > 1000)
                    {
                        MergeStagingToTarget(connection, targetSchema, targetTable, stagingTable, fromStatement);
                        DeleteRecordsFromTable(fullStagingTable, sortColumn, connection, log, bottomMerge);
                    }
                    else
                    {
                        MergeStagingToTarget(connection, targetSchema, targetTable, stagingTable);
                        DeleteRecordsFromTable(fullStagingTable, sortColumn, connection, log);
                    }
                }

            }
        }
    }

}

