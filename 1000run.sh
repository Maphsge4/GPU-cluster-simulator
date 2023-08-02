# non-placement
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --trace_file=input/input01/1000_job.csv --schedule=dlas-gpu --log_path=output/test_1 > output/raw/debug_dlas-gpu.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --trace_file=input/input01/1000_job.csv --schedule=dlas-gpu-gittins --log_path=output/test_1 > output/raw/debug_dlas-gpu-gittins.txt

# yarn
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=yarn --trace_file=input/input01/1000_job.csv --schedule=fifo --log_path=output/test_1 > output/raw/debug_fifo_yarn.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=yarn --trace_file=input/input01/1000_job.csv --schedule=fjf --log_path=output/test_1 > output/raw/debug_fjf_yarn.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=yarn --trace_file=input/input01/1000_job.csv --schedule=sjf --log_path=output/test_1 > output/raw/debug_sjf_yarn.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=yarn --trace_file=input/input01/1000_job.csv --schedule=lpjf --log_path=output/test_1 > output/raw/debug_lpjf_yarn.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=yarn --trace_file=input/input01/1000_job.csv --schedule=shortest-expected --log_path=output/test_1 > output/raw/debug_shortest-exp_yarn.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=yarn --trace_file=input/input01/1000_job.csv --schedule=shortest-gpu --log_path=output/test_1 > output/raw/debug_shortest-gpu_yarn.txt

# random  
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=random --trace_file=input/input01/1000_job.csv --schedule=fifo --log_path=output/test_1 > output/raw/debug_fifo_random.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=random --trace_file=input/input01/1000_job.csv --schedule=fjf --log_path=output/test_1 > output/raw/debug_fjf_random.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=random --trace_file=input/input01/1000_job.csv --schedule=sjf --log_path=output/test_1 > output/raw/debug_sjf_random.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=random --trace_file=input/input01/1000_job.csv --schedule=lpjf --log_path=output/test_1 > output/raw/debug_lpjf_random.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=random --trace_file=input/input01/1000_job.csv --schedule=shortest --log_path=output/test_1 > output/raw/debug_shortest_random.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=random --trace_file=input/input01/1000_job.csv --schedule=shortest-expected --log_path=output/test_1 > output/raw/debug_shortest-exp_random.txt

# count
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=count --trace_file=input/input01/1000_job.csv --schedule=fifo --log_path=output/test_1 > output/raw/debug_fifo_count.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=count --trace_file=input/input01/1000_job.csv --schedule=fjf --log_path=output/test_1 > output/raw/debug_fjf_count.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=count --trace_file=input/input01/1000_job.csv --schedule=sjf --log_path=output/test_1 > output/raw/debug_sjf_count.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=count --trace_file=input/input01/1000_job.csv --schedule=lpjf --log_path=output/test_1 > output/raw/debug_lpjf_count.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=count --trace_file=input/input01/1000_job.csv --schedule=shortest --log_path=output/test_1 > output/raw/debug_shortest_count.txt
python3 run_sim.py --cluster_spec=input/input01/n32g4.csv --print --scheme=count --trace_file=input/input01/1000_job.csv --schedule=shortest-expected --log_path=output/test_1 > output/raw/debug_shortest-exp_count.txt
