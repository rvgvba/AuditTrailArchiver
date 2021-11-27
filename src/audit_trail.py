import os
import bz2
from datetime import datetime
import pandas as pd


class AuditTrail:
    """
    used for sending pd.DataFrames to bz2 archive;
    useful for audit trail processes;
    """

    def __init__(self, archive_name: str, init_dataset: pd.DataFrame):
        self.archive_name = archive_name
        self.init_dataset = init_dataset
        self.hist_folder_name = 'archive_hist'
        self.extracted_dataset = None

        self.__create_hist_folder()

    @property
    def archive_name_complete(self):
        return f'{self.archive_name}.bin'

    def __create_hist_folder(self):
        """
        checks if there is any folder for hist archive files
        """
        if not os.path.exists(self.hist_folder_name):
            os.mkdir(self.hist_folder_name)

    def __get_arch_name_path(self) -> str:
        return os.path.join(self.hist_folder_name,
                            f'{AuditTrail.__date_format()}_archive_{self.archive_name_complete}')

    def __get_arch_custom_name(self, custom_year, custom_month) -> str:
        return os.path.join(self.hist_folder_name,
                            f'{custom_year}-{custom_month}_archive_{self.archive_name_complete}')

    def __pickle_dataset(self):
        self.init_dataset.to_pickle(self.archive_name)

    def get_extracted_data(self, arch_year, arch_month) -> pd.DataFrame:
        """
        reads data already extracted from archive (bin file);
        transforms the data from binary to functional pd.DataFrame;
        removes the extracted binary file;
        """
        self.extract_data(arch_year, arch_month)
        output_df = pd.read_pickle(self.archive_name)
        self.__remove_bin()
        print('Dataset extracted.')
        return output_df

    def __remove_bin(self):
        os.remove(self.archive_name)

    def archive_data(self):
        """
        transforms pd.DataFrame to binary;
        archives the binary file;
        removes the initial binary file;
        """
        self.__pickle_dataset()
        with open(self.archive_name, 'rb') as excel_input, \
                bz2.open(self.__get_arch_name_path(), 'wb') as excel_archived:
            excel_archived.write(excel_input.read())
        self.__remove_bin()

        print('Dataset archived.')

    def extract_data(self, c_year, c_month):
        """
        reads an already exiting archive and moves it to binary file
        """
        with bz2.open(self.__get_arch_custom_name(c_year, c_month), 'rb') as exiting_archive, \
                open(self.archive_name, 'wb') as new_excel:
            new_excel.write(exiting_archive.read())

    @staticmethod
    def __date_format() -> str:
        return datetime.now().strftime('%Y-%m')
