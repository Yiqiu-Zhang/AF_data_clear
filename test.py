import shutil
import os

base_folder = os.path.expanduser('~')+'/AF_data_clear/'

shutil.make_archive(base_folder+ 'distillation_dataset/pkl/pkl_10'.replace('pkl', 'msa'), 'tar', base_folder + 'distillation_dataset/pkl/pkl_10')