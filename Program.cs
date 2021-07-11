using Indusoft.TSDB.TSDBSDK;
using Microsoft.VisualBasic.FileIO;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Write2TSDB
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("**********************************************************************");
            Console.WriteLine(@"*    /\                                                              *");
            Console.WriteLine(@"*   / /\        генерация случайных значений в теги TSDB             *");
            Console.WriteLine(@"*  / / /\                                                            *");
            Console.WriteLine(@"*  \ \ \/       теги должны быть в файле TagList.csv                 *");
            Console.WriteLine(@"*   \ \/                                                             *");
            Console.WriteLine(@"*    \/                                                              *");
            Console.WriteLine("* InduSoft                                                           *");
            Console.WriteLine("**********************************************************************");
            try
            {
                List<string> TagList = new List<string>();
                using (TextFieldParser parser = new TextFieldParser(AppDomain.CurrentDomain.BaseDirectory + "TagList.csv"))
                {
                    parser.TextFieldType = FieldType.Delimited;
                    bool isFirst = true;
                    while (!parser.EndOfData)
                    {
                        string fields = parser.ReadLine();
                        TagList.Add(fields);
                    }

                }
                string TSDB_Name = ConfigurationManager.AppSettings["TSDB_Name"];
                TSDBServerConnection tsdb = TSDB_SDK.Instance.GetConnection(TSDB_Name);
                //Создание объекта для генерации чисел
                Random rnd = new Random();
                DateTime ts = DateTime.Now;
                foreach (string tag in TagList)
                {
                    try
                    {
                        double R = rnd.NextDouble();
                        tsdb.GetTagByName(tag).Data.UpdateValue(R * 1000, DateTime.Now);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine($"Ошибка с при записи в тег [{tag}]: {e.Message}");
                    }

                }
            }
            catch(Exception ex)
            {
                Console.WriteLine($"Произошла ошибка: {ex.Message}");
            }
        }
    }
}
