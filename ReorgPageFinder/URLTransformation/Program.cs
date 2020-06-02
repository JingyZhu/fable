using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;
using Microsoft.ProgramSynthesis.Transformation.Text;
using Microsoft.ProgramSynthesis.Transformation.Text.Semantics;
using Microsoft.ProgramSynthesis.Wrangling.Constraints;
using CsvHelper;

namespace URLTransformation
{
    public static class Constants{
        public const int numPrograms = 1;
    }

    class CSVStruct{
        public List<string> fields {get; set;}
        public List<List<string>> inputs {get; set;}
        public List<string> outputs {get; set;}
        public List<string> inferred {get; set;}

        public CSVStruct()
        {
            fields = new List<string>();
            outputs = new List<string>();
            inputs = new List<List<string>>();
            inferred = new List<string>();
        }

        public CSVStruct(string csvName, string outputField = "Output")
        {
            fields = new List<string>();
            outputs = new List<string>();
            inputs = new List<List<string>>();
            inferred = new List<string>();
            using (var reader = new StreamReader(csvName))
            using (var csvreader = new CsvReader(reader, CultureInfo.InvariantCulture))
            {    
                var records = csvreader.GetRecords<dynamic>();
                foreach (var record in records)
                {
                    IDictionary<string, object> recordDict = (IDictionary<string, object>)record;        
                    if (fields.Count == 0)
                    {
                        // Initialize Properties
                        foreach (string property in recordDict.Keys)
                        {
                            fields.Add(property);
                        }
                    }
                    inputs.Add(new List<string>());
                    foreach (string field in fields)
                    {
                        if (field == outputField){
                            outputs.Add(recordDict[field].ToString());
                        }
                        else{
                            inputs[inputs.Count - 1].Add(recordDict[field].ToString());
                        }
                    }
                }
            }
        }

        public void Print()
        {
            foreach (string field in fields)
            {
                Console.Write(field + "\t");
            }
            Console.Write("\n");
            foreach (int idx in Enumerable.Range(0, inputs.Count))
            {
                var value = inputs[idx];
                foreach (string v in value)
                {
                    Console.Write(v + "\t");
                }
                Console.Write(outputs[idx] + "\t");
                Console.Write("\n");
            }
        }

        public void WriteToCSV(string csvName)
        {
            using (var writer = new StreamWriter(csvName))
            {
                writer.Write(string.Join(',', fields));
                int ifr_idx = 0;
                foreach (int idx in Enumerable.Range(0, inputs.Count))
                {
                    if (outputs[idx] == ""){
                        writer.Write("\n{0},{1}", string.Join(',', inputs[idx]), inferred[ifr_idx]);
                        ifr_idx += 1;
                    }
                }
            }
        }
    }

    class URLTransformationProgram
    {
        static void Main(string[] args)
        {
            if (args.Length != 2){
                Console.WriteLine("Usage: URLTransformation <Input CSV> <Output Dir>");
                return;
            }
            string inputCSV = args[0];
            string dir = args[1];
            var ext  = Path.GetExtension(inputCSV);
            var name = Path.GetFileNameWithoutExtension(inputCSV);
            CSVStruct CSV = new CSVStruct(inputCSV);
            var session = new Session();
            var inputOutputPair = CSV.inputs.Zip(CSV.outputs, (i, o) => new {input=i, output=o});
            List<InputRow> needInfer = new List<InputRow>();
            // List<Example> constraints = new List<Example>();
            foreach (var iop in inputOutputPair)
            {
                if (iop.output != ""){
                    session.Constraints.Add(new Example(new InputRow(iop.input),iop.output));
                    // constraints.Add(new Example(new InputRow(iop.input),iop.output));
                }
                else {
                    needInfer.Add(new InputRow(iop.input));
                }
            }
            Program program = session.Learn();
            int output_count = 0;
            // foreach (var program in programs)
            // {
                CSV.inferred = new List<string>();
                foreach (var infer in needInfer)
                {
                    string output = program.Run(infer) as string;
                    CSV.inferred.Add(output);
                }
                string outputCSV = Path.Combine(dir, string.Format("{0}.out{1}{2}", name, output_count, ext));
                CSV.WriteToCSV(outputCSV);
                output_count += 1;
            // }
        }
    }
}
