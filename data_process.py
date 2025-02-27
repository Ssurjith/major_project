import pandas as pd
import re


class DataProcessor:

    def __init__(self, file_path):
        self.df = self.df = pd.read_excel(file_path)

    def convert_question_to_multiple_lines(self, row):
        # Split the text by the period followed by a space
        return f"{row['Document']}\n"

    def process(self):
        self.df['Question_clean'] = self.df.apply(lambda x: self.convert_question_to_multiple_lines(x), axis=1)
        return self.df


