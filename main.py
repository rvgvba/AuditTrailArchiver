import os
import pandas as pd
from src.audit_trail import AuditTrail


if __name__ == '__main__':

    # loading a dummy df
    dummy_df = pd.read_excel(os.path.join('dummy_file', 'file_example_XLSX_5000.xlsx'))

    # creating the object

    test_audit_trail = AuditTrail("accounting_file", dummy_df)
    test_audit_trail.archive_data()

    # read the acrhivve
    arch_df = test_audit_trail.get_extracted_data('2021', '11')