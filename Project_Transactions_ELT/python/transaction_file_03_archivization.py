import os
from pathlib import PurePath

def trans_file_archive(files_list):
    if files_list and len(files_list)>0:
        for f in files_list:
            os.rename(f,PurePath(f).parents[0] / "archived" / PurePath(f).name)