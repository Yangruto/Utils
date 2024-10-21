import re
import os
import dateutil
import gspread
import pandas as pd
import numpy as np
from gspread_dataframe import set_with_dataframe
from dateutil.relativedelta import relativedelta
from oauth2client.service_account import ServiceAccountCredentials as SAC

class Gsheet:
    def __init__(self, key_path):
        self.scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        self.key = SAC.from_json_keyfile_name(key_path, self.scope)
        self.client = gspread.authorize(self.key)

    def delete_worksheet(self, gsheet_id, worksheet_name):
        """
        Delete worksheet
            ghseet_id: gsheet id
            worksheet_name: worksheet name
        """
        spreadsheet = self.client.open_by_key(gsheet_id)
        spreadsheet.del_worksheet(worksheet_name)

    def get_worksheet_list(self, gsheet_id):
        """
        Get all worksheet name from gsheet
            gsheet_id: gsheet id
        """
        spreadsheet = self.client.open_by_key(gsheet_id)
        work_sheet_list = spreadsheet.worksheets()
        return work_sheet_list

    def gsheet2df(self, gsheet_id:str, worksheet_name:str):
        """
        Download google sheet as pandas dataframe
            gsheet_id: gsheet id
            worksheet_name: worksheet name
        """
        spreadsheet = self.client.open_by_key(gsheet_id)
        sheet = spreadsheet.worksheet(worksheet_name)
        df = sheet.get_all_values()
        df = pd.DataFrame(df[1:], columns=df[0])
        return df

    def append_gsheet(self, gsheet_id:str, worksheet_name:str, data:pd.DataFrame):
        """
        Append data to the gsheet
            gheet_id: gsheet id
            worksheet_name: worksheet name
            data: the data you want to append to the gsheet
        """
        spreadsheet = self.client.open_by_key(gsheet_id)
        sheet = spreadsheet.worksheet(worksheet_name)
        for k in range(len(data)):
            content = [i.item() if type(i)==np.int64 or type(i)==np.bool_ else i for i in data.loc[k].values]
            sheet.append_row(content)

    def replace_worksheet(self, gsheet_id:str, worksheet_name:str, data:pd.DataFrame):
        """
        Replace the whole worksheet data
            gsheet_id: gsheet id
            worksheet_name: worksheet name
            data: the data you want to replace to the worksheet
        """
        spreadsheet = self.client.open_by_key(gsheet_id)
        sheet = spreadsheet.worksheet(worksheet_name)
        sheet.clear()
        set_with_dataframe(sheet, data)