# 5 11 * * * /bin/bash /home/ubuntu/cron-script.sh

SHELL=/bin/bash
BASH_ENV=/home/ubuntu/.bashrc

source /home/ubuntu/miniconda3/etc/profile.d/conda.sh
conda activate env1

python /home/ubuntu/d/main.py >> /home/ubuntu/cron.log 2>&1