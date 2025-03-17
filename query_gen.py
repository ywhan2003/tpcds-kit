# https://zhuanlan.zhihu.com/p/671840400

import os


os.chdir("/home/ubuntu/tpcds-kit/tools")

output_dir = "/home/ubuntu/tpcds-queries"
query_temp_dir = "/home/ubuntu/tpcds-kit/query_templates"

count = 0 # count the number of queries
query_temps = os.listdir(query_temp_dir)

for query_temp in query_temps:
    if 'query' not in query_temp:
        continue
    print(query_temp)
    count += 1
    os.system(
        "./dsqgen -output_dir {} -scale 10 -directory ".format(output_dir)
        + query_temp_dir
        + " -template "
        + query_temp
        + " -DIALECT postgres -COUNT 1"
    )
    # rename the query file "query_0.sql" to "query_(temp).sql"
    os.system(
        "mv {}/query_0.sql {}/query_".format(output_dir, output_dir)
        + query_temp[5:-4]
        + ".sql"
    )
print(count)