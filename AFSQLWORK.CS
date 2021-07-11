using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AFHelper
{
    class AFSQLWORK
    {
        string AFSQLConnStr = "";
        string AFServerName = "";
        string AFDatabase = "";
        SqlConnection conn;
        #region QActiveEvents Запрос Активные события. Аргументы: StartTimeN, EndTimeN, AFDatabase, EFTemlate
        string QActiveEvents =
            "SET DATEFORMAT YMD;\n" +
            "DECLARE @StartTimeN as datetime\n" +
            "DECLARE @EndTimeN as datetime\n" +
            "DECLARE @AFDatabase as nvarchar(50)\n" +
            "DECLARE @EFTemlate as nvarchar(50)\n" +
            "SET @StartTimeN = '{0:yyyy-MM-dd HH:mm:ss}';\n" +
            "SET @EndTimeN = '{1:yyyy-MM-dd HH:mm:ss}';\n" +
            "SET @AFDatabase = '{2}'\n" +
            "SET @EFTemlate = '{3}'\n" +
            "IF NOT EXISTS(SELECT *\n" +
            "           FROM   sys.objects\n" +
            "           WHERE  object_id = OBJECT_ID(N'[dbo].[ToDateTime2]')\n" +
            "                  AND type IN ( N'FN', N'IF', N'TF', N'FS', N'FT' ))\n" +
            "BEGIN\n" +
            " DECLARE @sql NVARCHAR(MAX);\n" +
            " SET @sql = N'CREATE FUNCTION [dbo].[ToDateTime2] ( @Ticks bigint )\n" +
            "   RETURNS datetime2\n" +
            " AS\n" +
            " BEGIN\n" +
            "  DECLARE @DateTime datetime2 = ' + CHAR(39) + '00010101' + CHAR(39) +';\n" +
            "  SET @DateTime = DATEADD( DAY, @Ticks / 864000000000, @DateTime );\n" +
            "  SET @DateTime = DATEADD( SECOND, ( @Ticks % 864000000000) / 10000000, @DateTime );\n" +
            "  RETURN DATEADD( NANOSECOND, ( @Ticks % 10000000 ) * 100, @DateTime );\n" +
            " END'\n" +
            "\n" +
            " EXEC sp_executesql @sql;\n" +
            "END\n" +
            "\n" +
            "\n" +
            "DECLARE @starttime bigint\n" +
            "SET @starttime = [dbo].[DateToTicks](@StartTimeN);\n" +
            "\n" +
            "DECLARE @endtime bigint\n" +
            "SET @endtime = [dbo].[DateToTicks](@EndTimeN);\n" +
            "\n" +
            "DECLARE @tid int\n" +
            "SET @tid =\n" +
            "  (SELECT rid\n" +
            "   FROM AFElementTemplate\n" +
            "   WHERE id = (SELECT [id] FROM [dbo].[AFElementTemplate] WHERE Name = @EFTemlate AND fkdatabaseid = (SELECT [id] FROM [dbo].[AFDatabase] WHERE Name = @AFDatabase)));\n" +
            "\n" +
            "DECLARE @did int\n" +
            "SET @did =\n" +
            "  (SELECT rid\n" +
            "   FROM AFDatabase\n" +
            "   WHERE name = @AFDatabase);\n" +
            "\n" +
            "DECLARE @AllEF TABLE (\n" +
            " id uniqueidentifier INDEX IX1 CLUSTERED )\n" +
            "\n" +
            "             \n" +
            "\n" +
            "INSERT INTO @AllEF \n" +
            "SELECT DISTINCT \n" +
            "    ev.id\n" +
            "FROM\n" +
            "    [AFEventFrame] ev           \n" +
            "\n" +
            "    WHERE \n" +
            " (\n" +
            "        (ev.endtime > @starttime and ev.endtime < @endtime)\n" +
            "     OR\n" +
            "        (iscaptured = 0 and ev.endtime = 3155378975990000000)\n" +
            " )\n" +
            "    and ev.fkdatabaseid = @did\n" +
            "    and ev.fktemplateid = @tid  \n" +
            "                              \n" +
            "\n" +
            "DECLARE @FilteredEF0 TABLE (\n" +
            " id uniqueidentifier INDEX IX1 CLUSTERED )\n" +
            "\n" +
            "\n" +
            "\n" +
            "INSERT INTO @FilteredEF0\n" +
            "SELECT DISTINCT\n" +
            "    ev.id\n" +
            "FROM \n" +
            "    [AFEventFrame] ev\n" +
            "\n" +
            "\n" +
            "\n" +
            "WHERE(EXISTS (SELECT * from\n" +
            "                                            [AFEventFrame] ev1\n" +
            "                                            LEFT OUTER JOIN[dbo].[AFEventFrameAttributeValue] attrValue ON attrValue.fkeventframeid = ev1.Rowid\n" +
            "                                            LEFT OUTER JOIN[dbo].[AFElementTemplateAttribute] attrTempl ON attrValue.fkattributetemplateid = attrTempl.rid where ev1.Rowid = ev.Rowid \n" +
            "AND ((attrTempl.[name] = 'StatusCode' AND attrValue.valuestring = '1')\n" +
            "                             OR ev.endtime < 3155378975990000000  ))\n" +
            "\n" +
            "                                            \n" +
            "\n" +
            ")\n" +
            "\n" +
            "\n" +
            "AND\n" +
            "(\n" +
            "    (ev.endtime > @starttime and ev.endtime < @endtime)\n" +
            "    OR\n" +
            "    (iscaptured = 0 and ev.endtime = 3155378975990000000)\n" +
            ")\n" +
            "AND ev.fkdatabaseid = @did\n" +
            "AND ev.fktemplateid = @tid\n" +
            "SELECT\n" +
            "    res.id \n" +
            "FROM\n" +
            "    (\n" +
            "    \n" +
            "SELECT\n" +
            "    ae.id \n" +
            "FROM \n" +
            "    @AllEF ae INTERSECT SELECT \n" +
            "                                    ae.id \n" +
            "           FROM \n" +
            "               @AllEF ae\n" +
            "           LEFT JOIN\n" +
            "           @FilteredEF0 fe\n" +
            "           ON\n" +
            "           ae.id = fe.id\n" +
            "           WHERE fe.id is null\n" +
            "    ) AS res\n" +
            "    LEFT JOIN \n" +
            "        AFEventFrame \n" +
            "    ON\n" +
            "        res.id = AFEventFrame.id\n" +
            "    ORDER BY\n" +
            "        creationDate";
        #endregion
        #region QNotActiveEvents Запрос Неактивные события. Аргументы: StartTimeN, EndTimeN, AFDatabase, EFTemlate
        string QNotActiveEvents =
            "SET DATEFORMAT YMD;\n" +
            "DECLARE @StartTimeN as datetime\n" +
            "DECLARE @EndTimeN as datetime\n" +
            "DECLARE @AFDatabase as nvarchar(50)\n" +
            "DECLARE @EFTemlate as nvarchar(50)\n" +
            "SET @StartTimeN = '{0:yyyy-MM-dd HH:mm:ss}';\n" +
            "SET @EndTimeN = '{1:yyyy-MM-dd HH:mm:ss}';\n" +
            "SET @AFDatabase = '{2}'\n" +
            "SET @EFTemlate = '{3}'\n" +
            "IF NOT EXISTS(SELECT *\n" +
            "           FROM   sys.objects\n" +
            "           WHERE  object_id = OBJECT_ID(N'[dbo].[ToDateTime2]')\n" +
            "                  AND type IN ( N'FN', N'IF', N'TF', N'FS', N'FT' ))\n" +
            "BEGIN\n" +
            " DECLARE @sql NVARCHAR(MAX);\n" +
            " SET @sql = N'CREATE FUNCTION [dbo].[ToDateTime2] ( @Ticks bigint )\n" +
            "   RETURNS datetime2\n" +
            " AS\n" +
            " BEGIN\n" +
            "  DECLARE @DateTime datetime2 = ' + CHAR(39) + '00010101' + CHAR(39) +';\n" +
            "  SET @DateTime = DATEADD( DAY, @Ticks / 864000000000, @DateTime );\n" +
            "  SET @DateTime = DATEADD( SECOND, ( @Ticks % 864000000000) / 10000000, @DateTime );\n" +
            "  RETURN DATEADD( NANOSECOND, ( @Ticks % 10000000 ) * 100, @DateTime );\n" +
            " END'\n" +
            "\n" +
            " EXEC sp_executesql @sql;\n" +
            "END\n" +
            "\n" +
            "\n" +
            "DECLARE @starttime bigint\n" +
            "SET @starttime = [dbo].[DateToTicks](@StartTimeN);\n" +
            "\n" +
            "DECLARE @endtime bigint\n" +
            "SET @endtime = [dbo].[DateToTicks](@EndTimeN);\n" +
            "\n" +
            "DECLARE @tid int\n" +
            "SET @tid =\n" +
            "  (SELECT rid FROM AFElementTemplate\n" +
            "   WHERE id = (SELECT [id] FROM [dbo].[AFElementTemplate] WHERE Name = @EFTemlate AND fkdatabaseid = (SELECT [id] FROM [dbo].[AFDatabase] WHERE Name = @AFDatabase)));\n" +
            "\n" +
            "DECLARE @did int\n" +
            "SET @did = (SELECT rid FROM AFDatabase WHERE name = @AFDatabase);\n" +
            "\n" +
            "DECLARE @AllEF TABLE (id uniqueidentifier INDEX IX1 CLUSTERED )\n" +
            "\n" +
            "INSERT INTO @AllEF \n" +
            "SELECT DISTINCT \n" +
            "    ev.id\n" +
            "FROM\n" +
            "    [AFEventFrame] ev           \n" +
            "\n" +
            "    WHERE \n" +
            " (\n" +
            "        (ev.endtime > @starttime and ev.endtime < @endtime)\n" +
            "     OR\n" +
            "        (iscaptured = 0 and ev.endtime = 3155378975990000000)\n" +
            " )\n" +
            "    and ev.fkdatabaseid = @did\n" +
            "    and ev.fktemplateid = @tid  \n" +
            "                              \n" +
            "\n" +
            "DECLARE @FilteredEF0 TABLE (\n" +
            " id uniqueidentifier INDEX IX1 CLUSTERED )\n" +
            "\n" +
            "\n" +
            "\n" +
            "INSERT INTO @FilteredEF0\n" +
            "SELECT DISTINCT\n" +
            "    ev.id\n" +
            "FROM \n" +
            "    [AFEventFrame] ev\n" +
            "\n" +
            "\n" +
            "\n" +
            "WHERE(EXISTS (SELECT * from\n" +
            "                                            [AFEventFrame] ev1\n" +
            "                                            LEFT OUTER JOIN[dbo].[AFEventFrameAttributeValue] attrValue ON attrValue.fkeventframeid = ev1.Rowid\n" +
            "                                            LEFT OUTER JOIN[dbo].[AFElementTemplateAttribute] attrTempl ON attrValue.fkattributetemplateid = attrTempl.rid where ev1.Rowid = ev.Rowid \n" +
            "AND ((attrTempl.[name] = 'StatusCode' AND attrValue.valuestring = '1')\n" +
            "                             OR ev.endtime < 3155378975990000000  ))\n" +
            "                                            \n" +
            "\n" +
            ")\n" +
            "\n" +
            "\n" +
            "AND\n" +
            "(\n" +
            "    (ev.endtime > @starttime and ev.endtime < @endtime)\n" +
            "    OR\n" +
            "    (iscaptured = 0 and ev.endtime = 3155378975990000000)\n" +
            ")\n" +
            "AND ev.fkdatabaseid = @did\n" +
            "AND ev.fktemplateid = @tid\n" +
            "SELECT\n" +
            "    res.id \n" +
            "FROM\n" +
            "    (\n" +
            "    \n" +
            "SELECT\n" +
            "    ae.id \n" +
            "FROM \n" +
            "    @AllEF ae INTERSECT   SELECT id FROM @FilteredEF0\n" +
            "    ) AS res\n" +
            "    LEFT JOIN \n" +
            "        AFEventFrame \n" +
            "    ON\n" +
            "        res.id = AFEventFrame.id\n" +
            "    ORDER BY\n" +
            "        creationDate";
        #endregion
        #region QAcknowledgedEvents Запрос Квитированные события. Аргументы: StartTimeN, EndTimeN, AFDatabase, EFTemlate
        string QAcknowledgedEvents =
            "SET DATEFORMAT YMD;\n" +
            "DECLARE @StartTimeN as datetime\n" +
            "DECLARE @EndTimeN as datetime\n" +
            "DECLARE @AFDatabase as nvarchar(50)\n" +
            "DECLARE @EFTemlate as nvarchar(50)\n" +
            "SET @StartTimeN = '{0:yyyy-MM-dd HH:mm:ss}';\n" +
            "SET @EndTimeN = '{1:yyyy-MM-dd HH:mm:ss}';\n" +
            "SET @AFDatabase = '{2}'\n" +
            "SET @EFTemlate = '{3}'\n" +
            "IF NOT EXISTS(SELECT *\n" +
            "           FROM   sys.objects\n" +
            "           WHERE  object_id = OBJECT_ID(N'[dbo].[ToDateTime2]')\n" +
            "                  AND type IN ( N'FN', N'IF', N'TF', N'FS', N'FT' ))\n" +
            "BEGIN\n" +
            " DECLARE @sql NVARCHAR(MAX);\n" +
            " SET @sql = N'CREATE FUNCTION [dbo].[ToDateTime2] ( @Ticks bigint )\n" +
            "   RETURNS datetime2\n" +
            " AS\n" +
            " BEGIN\n" +
            "  DECLARE @DateTime datetime2 = ' + CHAR(39) + '00010101' + CHAR(39) +';\n" +
            "  SET @DateTime = DATEADD( DAY, @Ticks / 864000000000, @DateTime );\n" +
            "  SET @DateTime = DATEADD( SECOND, ( @Ticks % 864000000000) / 10000000, @DateTime );\n" +
            "  RETURN DATEADD( NANOSECOND, ( @Ticks % 10000000 ) * 100, @DateTime );\n" +
            " END'\n" +
            "\n" +
            " EXEC sp_executesql @sql;\n" +
            "END\n" +
            "\n" +
            "\n" +
            "DECLARE @starttime bigint\n" +
            "SET @starttime = 637012848370000000;\n" +
            "\n" +
            "DECLARE @endtime bigint\n" +
            "SET @endtime = 637013712370000000;\n" +
            "\n" +
            "DECLARE @tid int\n" +
            "SET @tid =\n" +
            "  (SELECT rid\n" +
            "   FROM AFElementTemplate\n" +
            "   WHERE id = (SELECT [id] FROM [dbo].[AFElementTemplate] WHERE Name = @EFTemlate AND fkdatabaseid = (SELECT [id] FROM [dbo].[AFDatabase] WHERE Name = @AFDatabase)));\n" +
            "\n" +
            "DECLARE @did int\n" +
            "SET @did = (SELECT rid FROM AFDatabase WHERE name = @AFDatabase);\n" +
            "\n" +
            "DECLARE @AllEF TABLE (id uniqueidentifier INDEX IX1 CLUSTERED )\n" +
            "\n" +
            "             \n" +
            "\n" +
            "INSERT INTO @AllEF \n" +
            "SELECT DISTINCT \n" +
            "    ev.id\n" +
            "FROM\n" +
            "    [AFEventFrame] ev           \n" +
            "\n" +
            "    WHERE \n" +
            " (\n" +
            "        (ev.endtime > @starttime and ev.endtime < @endtime)\n" +
            "     OR\n" +
            "        (iscaptured = 0 and ev.endtime = 3155378975990000000)\n" +
            " )\n" +
            "    and ev.fkdatabaseid = @did\n" +
            "    and ev.fktemplateid = @tid  \n" +
            "                              \n" +
            "\n" +
            "DECLARE @FilteredEF0 TABLE (\n" +
            " id uniqueidentifier INDEX IX1 CLUSTERED )\n" +
            "\n" +
            "\n" +
            "\n" +
            "INSERT INTO @FilteredEF0\n" +
            "SELECT DISTINCT\n" +
            "    ev.id\n" +
            "FROM \n" +
            "    [AFEventFrame] ev\n" +
            "\n" +
            "\n" +
            "\n" +
            "WHERE(EXISTS (SELECT * from\n" +
            "                                            [AFEventFrame] ev1\n" +
            "                                            LEFT OUTER JOIN[dbo].[AFEventFrameAttributeValue] attrValue ON attrValue.fkeventframeid = ev1.Rowid\n" +
            "                                            LEFT OUTER JOIN[dbo].[AFElementTemplateAttribute] attrTempl ON attrValue.fkattributetemplateid = attrTempl.rid where ev1.Rowid = ev.Rowid \n" +
            "AND ((attrTempl.[name] = 'Квитировано' AND attrValue.valuestring = '1')\n" +
            "                             ))\n" +
            "                                            \n" +
            "\n" +
            ")\n" +
            "\n" +
            "\n" +
            "AND\n" +
            "(\n" +
            "    (ev.endtime > @starttime and ev.endtime < @endtime)\n" +
            "    OR\n" +
            "    (iscaptured = 0 and ev.endtime = 3155378975990000000)\n" +
            ")\n" +
            "AND ev.fkdatabaseid = @did\n" +
            "AND ev.fktemplateid = @tid\n" +
            "SELECT\n" +
            "    res.id \n" +
            "FROM\n" +
            "    (\n" +
            "    \n" +
            "SELECT\n" +
            "    ae.id \n" +
            "FROM \n" +
            "    @AllEF ae INTERSECT   SELECT id FROM @FilteredEF0\n" +
            "    ) AS res\n" +
            "    LEFT JOIN \n" +
            "        AFEventFrame \n" +
            "    ON\n" +
            "        res.id = AFEventFrame.id\n" +
            "    ORDER BY\n" +
            "        creationDate";
        #endregion

        public AFSQLWORK(string _AFSQLConnStr, string _AFServerName, string _AFDatabase)
        {
            AFSQLConnStr = _AFSQLConnStr;
            AFServerName = _AFServerName;
            AFDatabase = _AFDatabase;
            conn = new SqlConnection(AFSQLConnStr);
            conn.Open();
        }

        public List<Guid> GetEventIDs(string Q, DateTime StartTime, DateTime EndTime, string EFTemplate)
        {
            SqlCommand cmd = new SqlCommand(string.Format(Q, StartTime, EndTime, AFDatabase, EFTemplate), conn);
            SqlDataReader dr = cmd.ExecuteReader(CommandBehavior.Default);
            List<Guid> ActiveEventIDs = (from IDataRecord r in dr select (Guid)r["id"]).ToList();
            return ActiveEventIDs;
        }
        public List<Guid> GetActiveEventIDs(DateTime StartTime, DateTime EndTime, string EFTemplate)
        {
            return GetEventIDs(QActiveEvents, StartTime, EndTime, EFTemplate);
        }
        public List<Guid> GetNotActiveEventIDs(DateTime StartTime, DateTime EndTime, string EFTemplate)
        {
            return GetEventIDs(QNotActiveEvents, StartTime, EndTime, EFTemplate);
        }
        public List<Guid> GetAcknowledgedEventIDs(DateTime StartTime, DateTime EndTime, string EFTemplate)
        {
            return GetEventIDs(QAcknowledgedEvents, StartTime, EndTime, EFTemplate);
        }
    }
}
