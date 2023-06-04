
mprof run src/music_project/main.py \
--file_name ./data/sample_listen-20230101.log \
--output_folder ./data/output \
--engine Polars
mprof plot -o polars_mem_perf.png -t 'memory vs time'

# mprof run src/music_project/main.py \
# --file_name ./data/sample_listen-20230101.log \
# --output_folder ./data/output \
# --engine Pandas
# mprof plot -o pandas_mem_perf.png -t 'memory vs time'