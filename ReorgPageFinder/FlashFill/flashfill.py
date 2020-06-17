import xlwings as xw
import string
import json, csv
import os
import pandas as pd
import pickle

class FlashFillHandler:
    def handle(self, inputs, identifier):
        """
        csvs: list of pickled dict with {'sheet_name': str, 'csv': dict for flashfill
        # TODO: Consider using protobuf for different language support
        """
        inputs = [pickle.loads(i.data) for i in inputs]
        csvs = [i['csv'] for i in inputs]
        sheet_names = [i['sheet_name'] for i in inputs]
        xlsx_path, output_cols = self.csv_xlsx(csvs, sheet_names, identifier)
        self.fill(xlsx_path, output_cols)
        outputs = self.xlsx_csv(xlsx_path)
        return [pickle.dumps(o) for o in outputs] 
    
    def fill(self, xlsx_path, output_cols, visible=False):
        """
        xlsx can have multiple sheet for higher throughput
        output_col: list(list), len=length of sheet. The #col for flashfill to be run
        """
        app = xw.App(visible=False)
        wb = app.books.open(xlsx_path)
        for cols, ws in zip(output_cols, wb.sheets): 
            assert(cols[-1] < 26)
            for col in cols:
                idx = string.ascii_uppercase[col]
                try:
                    r = ws.range(f'{idx}1')
                    r.api.flashfill()
                    wb.save()
                except Exception as e:
                    print('Flashfill:', str(e))
        app.kill()

    def csv_xlsx(self, csvs, sheet_names, identifier, output_name='Output'):
        """
        Merge received csvs into an xlsx with multiple sheets
        Reorder the Output to the end of the Column
        csvs: dict representing csv

        return: xlsx_path, output_cols
        """ 
        writer = pd.ExcelWriter(f'output\\{identifier}.xlsx')
        csvs = [pd.DataFrame(csv) for csv in csvs]
        output_cols, self.headers = [], {}
        for name, df in zip(sheet_names, csvs):
            cols = df.columns.tolist()
            assert(output_name in cols)
            input_col = [c for c in cols if output_name not in c]
            output_col = [c for c in cols if output_name in c]
            cols = input_col + output_col
            self.headers[name] = cols
            df = df[cols]
            output_cols.append(list(range(len(input_col), len(cols))))
            df.to_excel(writer, sheet_name=name, index=False, header=False)
        writer.save()
        return f"output\\{identifier}.xlsx", output_cols

    def xlsx_csv(self, xlsx_path):
        """
        Split different sheets in xlsx into different csvs --> dict

        returns: Same as input of csv_xlsx
        """ 
        excel = pd.read_excel(xlsx_path, header=None, sheet_name=None, engine='openpyxl')
        outputs = []
        for sheet_name, csv in excel.items():
            csv.columns = self.headers[sheet_name]
            outputs.append({
                'sheet_name': sheet_name,
                'csv': csv.to_dict(orient='list')
            })
        os.remove(xlsx_path)
        return outputs