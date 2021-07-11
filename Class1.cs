using System;
using Indusoft.Ceng.DynamicCode.Common;
using Indusoft.Ceng.TabularData.Plugin;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using Indusoft.Ceng.Psc.Common;
using System.Linq;
using Indusoft.TSDB.TSDBSDK;
using Indusoft.Ceng.Uds.Common;
using System.Diagnostics;
// Глазырин С.И. 2018-10-11
public class ARMProductGroup : IDcCalculate
{
    //private static readonly Dictionary<string, string> EngUnits = new Dictionary<string, string>();
    const string PI_Name = "ONOS-SPI01";
    const string TSDB_Name = "TSDB";
    const string connStrISP = "Data Source=ONOS-SPI02;Initial Catalog=ISP;uid=sa;pwd=I!Soft@ONOS#;MultipleActiveResultSets=True";

    const string GetAllTagsQuery =
    "WITH InsQuery1 As\n" +
    "(\n" +
    "SELECT El.[ObjectId] As ElID\n" +
    "      ,El.[Name] As TankName\n" +
    "    ,ElAt.ObjectId As AtrId\n" +
    "    ,Convert(float, TmVl.Value) As MaxMass\n" +
    "    ,TmVl.Time As MassTime\n" +
    "    ,ElAt.ObjectId As MaxMassObjectId\n" +
    "    ,ROW_NUMBER() OVER (PARTITION BY El.[Name] ORDER BY TmVl.Time DESC) RowNum\n" +
    "  FROM [ISP].[dbo].[Element] as El,\n" +
    "       [ISP].[dbo].[ElementTemplate] as ElTp,\n" +
    "       [ISP].[dbo].[ElementAttribute] as ElAt,\n" +
    "     [ISP].[dbo].[TimedValue] as TmVl\n" +
    "  WHERE El.ElementTemplateId = ElTp.ObjectId\n" +
    "    AND El.ObjectId = ElAt.ElementId\n" +
    "  AND ElAt.ObjectId = TmVl.ElementAttributeId\n" +
    "    AND El.IsDeleted = 0\n" +
    "    AND ElTp.Name = 'Резервуар'\n" +
    "  AND ElAt.Name = 'Масса максимальная'\n" +
    "    AND Convert(float, TmVl.Value) > 0\n" +
    "),\n" +
    "MASSA_OBSH AS\n" +
    "(\n" +
    "SELECT E.ObjectId , E.Name [ELName], EA.NAME, X.Properties.value('@ServerName', 'varchar(20)') AS 'ServerName',  X.Properties.value('@Tag', 'varchar(150)') AS 'Tag' \n" +
    "FROM[ISP].[dbo].[Element] E LEFT JOIN[ISP].[dbo].[ElementAttribute] EA  ON E.ObjectId = EA.ElementId\n" +
    "CROSS APPLY[DataReferenceProperties].nodes('Properties') AS X(Properties)\n" +
    "WHERE EA.Name='Масса общая'\n" +
    "),\n" +
    "PRODUCT AS\n" +
    "(\n" +
    "SELECT E.ObjectId , E.Name [ELName], EA.NAME, X.Properties.value('@ServerName', 'varchar(20)') AS 'ServerName',  X.Properties.value('@Tag', 'varchar(150)') AS 'Tag' \n" +
    "FROM[ISP].[dbo].[Element] E LEFT JOIN[ISP].[dbo].[ElementAttribute] EA  ON E.ObjectId = EA.ElementId\n" +
    "CROSS APPLY[DataReferenceProperties].nodes('Properties') AS X(Properties)\n" +
    "WHERE EA.Name='Продукт'\n" +
    ")\n" +
    "SELECT [TankName],[MaxMass],MASSA_OBSH.ServerName,MASSA_OBSH.Tag,PRODUCT.ServerName,PRODUCT.Tag\n" +
    "  FROM InsQuery1\n" +
    "LEFT JOIN [ISP].[dbo].[ElementAttribute] as ElAt2 ON (ElID = ElAt2.ElementId AND ElAt2.Name = 'Продукт')\n" +
    "LEFT JOIN [ISP].[dbo].[ElementAttribute] as ElAt3 ON (ElID = ElAt3.ElementId AND ElAt3.Name = 'Масса общая')\n" +
    "LEFT JOIN MASSA_OBSH ON MASSA_OBSH.ObjectId = [ElID]\n" +
    "LEFT JOIN PRODUCT ON PRODUCT.ObjectId = [ElID]\n" +
    "WHERE RowNum=1\n";
    /*
	   возвращает такую таблицу:
	   
	   	TankName			|MaxMass	ServerName	Tag						ServerName	Tag
	   	____________________|______________________________________________________________________________
		ТСБ Резервуар №393	|8265		ONOS-SPI01	ONOS.T10.U2XXXR:393.MS	ONOS-SPI01	ONOS.T10.U2XXXR:393.PG
		ЭСН Резервуар №449	|2400		ONOS-SPI01	ONOS.T10.U1ESTR:449.MS	ONOS-SPI01	ONOS.T10.U1ESTR:449.PG
		ТСБ Резервуар №06	|2652		ONOS-SPI01	ONOS.T10.U2XXXR:006.MS	ONOS-SPI01	ONOS.T10.U2XXXR:006.PG
		ОХМ Резервуар №84	|3900		ONOS-SPI01	ONOS.T10.U1OMZR:084.MS	ONOS-SPI01	ONOS.T10.U1OMZR:084.PG
	*/
    const string GetMassByGroupQuery =
    "Declare @TMMP AS TABLE(TankName nvarchar(max), MaxMass float, Mass float, Product nvarchar(max))\n" +
    "insert into @TMMP values \n" +
    "{0} \n" +
    "WITH A AS\n" +
    "(\n" +
    "SELECT DictItem.Name As ProdName, DictItem.Description As ProdGroup\n" +
    "FROM [ISP].[dbo].[Dictionary] as Dict,\n" +
    "[ISP].[dbo].[Dictionary] as Dict2,\n" +
    "[ISP].[dbo].[DictionaryItem] as DictItem,\n" +
    "[ISP].[dbo].[DictionaryItem] as DictItem2\n" +
    "WHERE Dict.ObjectId = DictItem.DictionaryId \n" +
    "AND Dict2.Name = 'ARMProductGroups'\n" +
    "AND DictItem.Description = DictItem2.Name\n" +
    "AND Dict2.ObjectId = DictItem2.DictionaryId\n" +
    ")\n" +
    "SELECT Sum(MaxMass) [MaxMass],  Sum(Mass) [CommMass], ProdGroup FROM @TMMP T LEFT JOIN A ON T.Product = A.ProdName\n" +
    "WHERE ProdGroup Is Not NULL\n" +
    "Group BY ProdGroup\n";
    /* возваращает табличку типа:
	MaxMass	CommMass	ProdGroup
	_______________________________________________________
	16305	10274,8725	Бензины высокооктановые компоненты
	13250	5597,85336	Бензины высокооктановые товарные
	9605	4174,301	Бензины низкооктановые компоненты
	8200	6377,3605	Бензины низкооктановые товарные
	46472	39415,747	Вакуумные дистилляты
	13780	8768,6312	Газовый конденсат
	*/
    public readonly Dictionary<string, string> OutParamsByProducts = new Dictionary<string, string>();
    public readonly Dictionary<string, string> MaxParamsByProducts = new Dictionary<string, string>();

    public void Prepare(IDcContext context, DcRunProterties dcRunProterties)
    {
        try
        {
            OutParamsByProducts.Add("Нефть", "Out01");
            OutParamsByProducts.Add("Газовый конденсат", "Out02");
            OutParamsByProducts.Add("МТБЭ", "Out03");
            OutParamsByProducts.Add("ОКТА плюс", "Out04");
            OutParamsByProducts.Add("Бензины низкооктановые товарные", "Out06");
            OutParamsByProducts.Add("Бензины низкооктановые компоненты", "Out07");
            OutParamsByProducts.Add("Бензины высокооктановые товарные", "Out08");
            OutParamsByProducts.Add("Бензины высокооктановые компоненты", "Out09");
            OutParamsByProducts.Add("Топлива РТ", "Out10");
            OutParamsByProducts.Add("Топливо дизельное класса 5", "Out11");
            OutParamsByProducts.Add("Топливо дизельное класса 4", "Out12");
            OutParamsByProducts.Add("Топливо дизельное класса 3", "Out13");
            OutParamsByProducts.Add("Компоненты ДТ", "Out14");
            OutParamsByProducts.Add("Мазуты", "Out15");
            OutParamsByProducts.Add("Вакуумные дистилляты", "Out16");
            OutParamsByProducts.Add("Нефтебитум", "Out17");
            OutParamsByProducts.Add("Сера", "Out18");

            MaxParamsByProducts.Add("Нефть", "Max01");
            MaxParamsByProducts.Add("Газовый конденсат", "Max02");
            MaxParamsByProducts.Add("МТБЭ", "Max03");
            MaxParamsByProducts.Add("ОКТА плюс", "Max04");
            MaxParamsByProducts.Add("Бензины низкооктановые товарные", "Max06");
            MaxParamsByProducts.Add("Бензины низкооктановые компоненты", "Max07");
            MaxParamsByProducts.Add("Бензины высокооктановые товарные", "Max08");
            MaxParamsByProducts.Add("Бензины высокооктановые компоненты", "Max09");
            MaxParamsByProducts.Add("Топлива РТ", "Max10");
            MaxParamsByProducts.Add("Топливо дизельное класса 5", "Max11");
            MaxParamsByProducts.Add("Топливо дизельное класса 4", "Max12");
            MaxParamsByProducts.Add("Топливо дизельное класса 3", "Max13");
            MaxParamsByProducts.Add("Компоненты ДТ", "Max14");
            MaxParamsByProducts.Add("Мазуты", "Max15");
            MaxParamsByProducts.Add("Вакуумные дистилляты", "Max16");
            MaxParamsByProducts.Add("Нефтебитум", "Max17");
            MaxParamsByProducts.Add("Сера", "Max18");
        }
        catch
        {

        }

    }
    public void Initialization(IDcContext context)
    {

    }

    public void EntryPointDebug(IDcContext context)
    {
        Debugger.Break();
        context.Log("==> EntryPointDebug <==");
    }

    public void Register(IDcRegister register)
    {
        /*
	        1	Нефть
			2	Газовый конденсат
			3	МТБЭ
			4	ОКТА плюс
			6	Бензины низкооктановые товарные
			7	Бензины низкооктановые компоненты
			8	Бензины высокооктановые товарные
			9	Бензины высокооктановые компоненты
			10	Топлива РТ
			11	Топливо дизельное класса 5
			12	Топливо дизельное класса 4
			13	Топливо дизельное класса 3
			14	Компоненты ДТ
			15	Мазуты
			16	Вакуумные дистилляты
			17	Нефтебитум
			18	Сера

        */
        register.Add("Out01", DcValueTypeEnum.Double, "Общая масса (Нефть)");
        register.Add("Out02", DcValueTypeEnum.Double, "Общая масса (Газовый конденсат)");
        register.Add("Out03", DcValueTypeEnum.Double, "Общая масса (МТБЭ)");
        register.Add("Out04", DcValueTypeEnum.Double, "Общая масса (ОКТА плюс)");
        register.Add("Out06", DcValueTypeEnum.Double, "Общая масса (Бензины низкооктановые товарные)");
        register.Add("Out07", DcValueTypeEnum.Double, "Общая масса (Бензины низкооктановые компоненты)");
        register.Add("Out08", DcValueTypeEnum.Double, "Общая масса (Бензины высокооктановые товарные)");
        register.Add("Out09", DcValueTypeEnum.Double, "Общая масса (Бензины высокооктановые компоненты)");
        register.Add("Out10", DcValueTypeEnum.Double, "Общая масса (Топлива РТ)");
        register.Add("Out11", DcValueTypeEnum.Double, "Общая масса (Топливо дизельное класса 5)");
        register.Add("Out12", DcValueTypeEnum.Double, "Общая масса (Топливо дизельное класса 4)");
        register.Add("Out13", DcValueTypeEnum.Double, "Общая масса (Топливо дизельное класса 3)");
        register.Add("Out14", DcValueTypeEnum.Double, "Общая масса (Компоненты ДТ)");
        register.Add("Out15", DcValueTypeEnum.Double, "Общая масса (Мазуты)");
        register.Add("Out16", DcValueTypeEnum.Double, "Общая масса (Вакуумные дистилляты)");
        register.Add("Out17", DcValueTypeEnum.Double, "Общая масса (Нефтебитум)");
        register.Add("Out18", DcValueTypeEnum.Double, "Общая масса (Сера)");

        register.Add("Max01", DcValueTypeEnum.Double, "Максимальная масса (Нефть)");
        register.Add("Max02", DcValueTypeEnum.Double, "Максимальная масса (Газовый конденсат)");
        register.Add("Max03", DcValueTypeEnum.Double, "Максимальная масса (МТБЭ)");
        register.Add("Max04", DcValueTypeEnum.Double, "Максимальная масса (ОКТА плюс)");
        register.Add("Max06", DcValueTypeEnum.Double, "Максимальная масса (Бензины низкооктановые товарные)");
        register.Add("Max07", DcValueTypeEnum.Double, "Максимальная масса (Бензины низкооктановые компоненты)");
        register.Add("Max08", DcValueTypeEnum.Double, "Максимальная масса (Бензины высокооктановые товарные)");
        register.Add("Max09", DcValueTypeEnum.Double, "Максимальная масса (Бензины высокооктановые компоненты)");
        register.Add("Max10", DcValueTypeEnum.Double, "Максимальная масса (Топлива РТ)");
        register.Add("Max11", DcValueTypeEnum.Double, "Максимальная масса (Топливо дизельное класса 5)");
        register.Add("Max12", DcValueTypeEnum.Double, "Максимальная масса (Топливо дизельное класса 4)");
        register.Add("Max13", DcValueTypeEnum.Double, "Максимальная масса (Топливо дизельное класса 3)");
        register.Add("Max14", DcValueTypeEnum.Double, "Максимальная масса (Компоненты ДТ)");
        register.Add("Max15", DcValueTypeEnum.Double, "Максимальная масса (Мазуты)");
        register.Add("Max16", DcValueTypeEnum.Double, "Максимальная масса (Вакуумные дистилляты)");
        register.Add("Max17", DcValueTypeEnum.Double, "Максимальная масса (Нефтебитум)");
        register.Add("Max18", DcValueTypeEnum.Double, "Максимальная масса (Сера)");

    }

    public void Calculate(IDcContext context)
    {
        DateTime TIMESTAMP = DateTime.Now;
        context.Log(String.Format("[ARMProductGroup] начало расчёта"));
        context.Log(String.Format("сейчас будет выполнен запрос 1 :"));
        context.Log(String.Format("--------------------------------------------------"));
        context.Log(String.Format("{0}", GetAllTagsQuery));
        context.Log(String.Format("--------------------------------------------------"));
        try
        {
            //откроем соединения

            PISDK.PISDK PiSdk = new PISDK.PISDK();
            PISDK.Server PI = PiSdk.Servers[PI_Name];
            PI.Open();

            TSDBServerConnection tsdb = TSDB_SDK.Instance.GetConnection(TSDB_Name);

            string QueryPart = "";
            // работа с SQL
            SqlConnection conn = new SqlConnection(connStrISP);
            conn.Open();
            SqlCommand cmd = new SqlCommand(GetAllTagsQuery, conn);

            SqlDataReader dr = cmd.ExecuteReader(CommandBehavior.Default);

            while (dr.Read())
            {
                string TankName = dr.GetValue(0).ToString();
                double MaxMass = Convert.ToDouble(dr.GetValue(1).ToString());
                string MassServerName = dr.GetValue(2).ToString();
                string MassTag = dr.GetValue(3).ToString();

                string ProdServerName = dr.GetValue(4).ToString();
                string ProdTag = dr.GetValue(5).ToString();
                string Product = "";
                double Mass = 0;
                // Общая масса
                if (MassServerName == PI_Name)
                {
                    try
                    {
                        Mass = Convert.ToDouble(PI.PIPoints[MassTag].Data.Snapshot.Value.ToString());
                    }
                    catch (Exception e)
                    {
                        context.Log(String.Format("Ошибка при чтении тега PI [{0}] ({1}): {2}", MassTag, TankName, e.Message));
                    }
                }
                else if (MassServerName == TSDB_Name)
                {
                    try
                    {
                        Mass = Convert.ToDouble(tsdb.GetTagByName(MassTag).Data.ArcValue(TIMESTAMP, RetrievalTypeConstants.rtAtOrBefore).Value);
                    }
                    catch (Exception e)
                    {
                        context.Log(String.Format("Ошибка при чтении тега TSDB [{0}] ({1}): {2}", MassTag, TankName, e.Message));
                    }

                }
                else
                {
                    context.Log(String.Format("Не указан тип сервера для тега массы общей [{0}]", TankName));
                }
                // продукт
                if (ProdServerName == PI_Name)
                {
                    try
                    {
                        if (PI.PIPoints[ProdTag].PointType == PISDK.PointTypeConstants.pttypDigital)
                            Product = (PI.PIPoints[ProdTag].Data.Snapshot.Value as PISDK.DigitalState).Name;
                    }
                    catch (Exception e)
                    {
                        context.Log(String.Format("Ошибка при чтении тега PI [{0}] ({1}): {2}", ProdTag, TankName, e.Message));
                    }
                }
                else if (ProdServerName == TSDB_Name)
                {
                    try
                    {
                        Product = tsdb.GetTagByName(ProdTag).Data.ArcValue(TIMESTAMP, RetrievalTypeConstants.rtAtOrBefore).Value.ToString();
                    }
                    catch (Exception e)
                    {
                        context.Log(String.Format("Ошибка при чтении тега TSDB [{0}] ({1}): {2}", ProdTag, TankName, e.Message));
                    }

                }
                else
                {
                    context.Log(String.Format("Не указан тип сервера для тега продукта [{0}]", TankName));
                }

                //context.Log(String.Format("{0} | {1} | {2} | {3} | {4} | {5}", dr.GetValue(0).ToString(), dr.GetValue(1).ToString(), dr.GetValue(2).ToString(), dr.GetValue(3).ToString(), dr.GetValue(4).ToString(), dr.GetValue(5).ToString())); 
                //context.Log(String.Format("{0} | {1} | {2} | {3} ", TankName, MaxMass, Mass, Product));
                // context.Log(String.Format("('{0}', {1}, {2} , '{3}'), ", TankName, MaxMass, Mass, Product));
                QueryPart += String.Format("('{0}', {1}, {2} , '{3}'),", TankName, MaxMass, Mass, Product); //"('ТСБ Резервуар №393', 8265, 8594.892 , 'Топливо нефтяное вакуумной перегонки'), \n"+
            }


            PI.Close();

            int last_sym_no = QueryPart.Length - 1;
            QueryPart = QueryPart.Remove(last_sym_no); // удаляем последнюю запятую
            QueryPart += ";";

            context.Log(String.Format("сейчас будет выполнен запрос 2 :"));
            context.Log(String.Format("--------------------------------------------------"));
            context.Log(String.Format("{0}", String.Format(GetMassByGroupQuery, QueryPart)));
            context.Log(String.Format("--------------------------------------------------"));

            SqlCommand cmd2 = new SqlCommand(String.Format(GetMassByGroupQuery, QueryPart), conn);

            SqlDataReader drr = cmd2.ExecuteReader(CommandBehavior.CloseConnection);
            while (drr.Read())
            {
                double MMass = Convert.ToDouble(drr.GetValue(0).ToString());
                double CMass = Convert.ToDouble(drr.GetValue(1).ToString());
                string PG = drr.GetValue(2).ToString();
                string OutParamenerName = "";
                context.Log(String.Format("По группе [{0}] Макс Масса [{1:0.000}] Общая Масса [{2:0.000}]", PG, MMass, CMass));
                if (OutParamsByProducts.TryGetValue(PG, out OutParamenerName))
                {
                    context.Attribute(OutParamenerName).SaveValue<double>(CMass, TIMESTAMP, true);
                }
                if (MaxParamsByProducts.TryGetValue(PG, out OutParamenerName))
                {
                    context.Attribute(OutParamenerName).SaveValue<double>(MMass, TIMESTAMP, true);
                }
            }
            conn.Close();
            conn.Dispose();
        }
        catch (Exception exc)
        {
            context.Log(String.Format("Произошла ошибка: {0}", exc.Message));
        }
        context.Log(String.Format("[ARMProductGroup] конец расчёта"));
    }

    public void Finalization(IDcContext context)
    {

    }

}