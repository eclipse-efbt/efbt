import os

PATH_RESULTS = "birds_nest/results/database_export"
col_index = dict()
for file_path in os.listdir(PATH_RESULTS):
    with open(PATH_RESULTS+os.sep+file_path) as f:
        col_index[file_path] = dict(enumerate(list(map(str.lower,f.read().split("\n")[0].split(",")))))

import json

with open("result_folder.json", "w") as f:
    json.dump(col_index, f)
