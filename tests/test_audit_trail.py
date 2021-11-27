import unittest
import pandas as pd
import numpy as np
from src.audit_trail import AuditTrail

df2 = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]), columns=['a', 'b', 'c'])


class TestAuditTrail(unittest.TestCase):

    def setUp(self) -> None:
        self.audit_obj = AuditTrail('test_archive', pd.DataFrame())
        self.audit_obj_ok = AuditTrail('accounting_file', df2)

    def test_empty_df(self):
        with self.assertRaises(ValueError):
            self.audit_obj.archive_data()

    def test_wrong_archive(self):
        with self.assertRaises(FileExistsError):
            self.audit_obj.extract_data(2020, 1)

    def test_wrong_extract_info(self):
        with self.assertRaises(FileExistsError):
            self.audit_obj.extract_data('wrong_year', 'wrong_month')



if __name__ == '__main__':
    unittest.main()
