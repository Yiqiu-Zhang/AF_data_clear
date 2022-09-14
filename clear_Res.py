import os
import pickle
from os import listdir
from os.path import isfile, join
import numpy as np
import shutil
import multiprocessing



ID_TO_HHBLITS_AA = {
    0: 'A',
    1: 'C',  # Also U.
    2: 'D',  # Also B.
    3: 'E',  # Also Z.
    4: 'F',
    5: 'G',
    6: 'H',
    7: 'I',
    8: 'K',
    9: 'L',
    10: 'M',
    11: 'N',
    12: 'P',
    13: 'Q',
    14: 'R',
    15: 'S',
    16: 'T',
    17: 'V',
    18: 'W',
    19: 'Y',
    20: 'X',  # Includes J and O.
    21: '-'
}
PROCESS_NUM = 1
base_folder = os.path.expanduser('~')+'/AF_data_clear/'
bucket_base = 's3://AF_data/'

def upload_item(folder_name):

    upload_command = 'aws s3 cp %s%s %s%s' % (base_folder, folder_name, bucket_base, folder_name.replace('pkl', 'msa'))
    print(upload_command)
    os.system(upload_command)

def convert(process_name_list):

    while len(process_name_list) > 0:
        pkl_file = process_name_list.pop(0)
        print(pkl_file)
        with open(base_folder + pkl_file, 'rb') as f:
            features = pickle.load(f)
            f.close()
        os.remove(base_folder + pkl_file)

        msa_name = '.'.join([pkl_file.split('.')[0], 'aln'])
        with open(base_folder + msa_name, 'w') as f:
            msa_array = np.vectorize(ID_TO_HHBLITS_AA.get)(features['msa'])
            f.write('\n'.join(''.join(seq) for seq in msa_array))
            f.close()
        upload_item(msa_name)
        os.remove(base_folder + msa_name)

def extract_clear(tar_list):

    for tar_name in tar_list:

        name_list = [tar_name + '/' + name for name in listdir(tar_name) if isfile(join(tar_name, name))]

        if 'pkl' in tar_name:

            convert(name_list)


        else:  # 是pdb文件 直接上传整个文件夹并删除文件夹

            os.system(f'aws s3 cp {base_folder}{tar_name} {bucket_base}{tar_name} --recursive')
            shutil.rmtree(base_folder + tar_name)

if __name__ == '__main__':
    dir_name = 'distillation_dataset/'
    #tar_list = ['distillation_dataset/'+ name for name in os.walk(dir_name)]
    tar_list = []
    for (dir_path, dir_names, file_names) in os.walk(dir_name):
        tar_list.extend(file_names)
    print(tar_list)
    # extract_clear(tar_list)

