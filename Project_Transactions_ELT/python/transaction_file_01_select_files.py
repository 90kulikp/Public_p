import os
import re
from pathlib import PurePath

def trans_file_E():
    parent_dir = PurePath(__file__).parents[1]
    raw_dir = parent_dir / 'raw'
    file_list = os.listdir(raw_dir)
   
    reg_pattern = re.compile(r'sales_transactions_20[0-9][0-9][0-1][0-9][0-3][0-9]_[0-2][0-9][0-5][0-9][0-5][0-9].csv')
    files_to_import = [os.path.join(raw_dir, i) for i in file_list if reg_pattern.match(i)]

    return(files_to_import)