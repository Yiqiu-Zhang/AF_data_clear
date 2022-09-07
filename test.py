import shutil
import os

base_folder = os.path.expanduser('~')+'/AF_data_clear/'

os.system(f'aws s3 cp s3://AF_data/distillation_dataset/msa/msa_0 {base_folder}distillation_dataset/msa/msa_0 --recursive')

shutil.make_archive(f'{base_folder}distillation_dataset/msa/msa_0.tar', 'tar',  f'{base_folder}distillation_dataset/msa/msa_0')