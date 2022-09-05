import os
import pickle
from os import listdir
from os.path import isfile, join
import numpy as np

bucket_base = 's3://AF_data'
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


def upload_item(file_name):
    upload_command = 'aws s3 cp %s/%s %s/%s' % (file_name, '.tar.gz', bucket_base, file_name)
    print(upload_command)
    os.system(upload_command)


def extract_clear(tar_list):
    for tar_name in tar_list:
        target_dir = '/'.join(tar_name.split('/')[:-1])

        #os.system(f'aws s3 cp s3://AF_data_jinzhen/{tar_name} {base_folder}/{tar_name}')
        #os.system(f'tar -xvf {base_folder}/{tar_name} -C {base_folder}/{target_dir}')
        #os.remove(base_folder + tar_name)

        pack = tar_name.split('.')[0]

        if 'pkl' in tar_name:
            pkl_list = [pack+'/'+pkl_name for pkl_name in listdir(pack) if isfile(join(pack, pkl_name))]

            for pkl_file in pkl_list:
                print(pkl_file)
                with open(base_folder+pkl_file, 'rb') as f:
                    features = pickle.load(f)
                os.remove(base_folder+pkl_file)

                msa_name = '.'.join([pkl_file.split('.')[0], 'aln'])
                with open(base_folder+msa_name, 'w') as f:
                    f.write('\n'.join(''.join([ID_TO_HHBLITS_AA[val] for val in row]) for row in features['msa']))
                    f.close()

        upload_item(pack)



tar_list = [item.strip() for item in open('tar_file_name.txt').readlines()]

tar_list = ['distillation_dataset/pkl/pkl_9.tar.gz']
base_folder = os.path.expanduser('~')+'/AF_data_clear/'
if __name__ == '__main__':

    extract_clear(tar_list)

