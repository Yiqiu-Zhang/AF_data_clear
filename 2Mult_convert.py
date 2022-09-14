import threading
import os
import pickle
from os import listdir
from os.path import isfile, join
import numpy as np
import shutil
import multiprocessing

base_folder = os.path.expanduser('~')+'/AF_data_clear/'
bucket_base = 's3://AF_data/'

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


def upload_item(folder_name):

    upload_command = 'aws s3 cp %s%s %s%s' % (base_folder, folder_name, bucket_base, folder_name.replace('pkl', 'msa'))
    print(upload_command)
    os.system(upload_command)


def extract_clear(tar_list):

    for tar_name in tar_list:
        pack = tar_name.split('.')[0] # e.g distillation_dataset/pkl/pkl_162    .tar.gz

        os.system(f'aws s3 cp s3://AF_data/finished_file.txt {base_folder}finished_file.txt')

        with open(f'{base_folder}finished_file.txt', 'r') as f: # 下载已完成文件目录
            found = f.read().split('\n')


        if tar_name not in found: # 检测是否已完成

            # 添加文件到已完成
            with open(f'{base_folder}/finished_file.txt', 'a') as f:
                f.write(f'\n{tar_name}')
                f.close()
            os.system(f'aws s3 cp {base_folder}finished_file.txt s3://AF_data/finished_file.txt ')

            target_dir = '/'.join(tar_name.split('/')[:-1])

            os.system(f'aws s3 cp s3://AF_data_jinzhen/{tar_name} {base_folder}/{tar_name}')
            os.system(f'tar -xvf {base_folder}/{tar_name} -C {base_folder}/{target_dir}')
            os.remove(base_folder + tar_name) # 移除压缩文件


            name_list = [pack + '/' + name for name in listdir(pack) if isfile(join(pack, name))]


            if 'pkl' in tar_name:

                threads = []
                sub_l = len(name_list) // PROCESS_NUM
                for n in range(PROCESS_NUM+1):
                    sub_process = multiprocessing.Process(target=convert, args=(name_list[n * sub_l:(n + 1) * sub_l],))
                    threads.append(sub_process)

                for x in threads:
                    x.start()
                for x in threads:
                    x.join()

            else: # 是pdb文件 直接上传整个文件夹并删除文件夹

                os.system(f'aws s3 cp {base_folder}{pack} {bucket_base}{pack} --recursive')
                shutil.rmtree(base_folder + pack)



# tar_list = [item.strip() for item in open('tar_file_name.txt').readlines()]

tar_list = ['distillation_dataset/pkl/pkl_150',
            'distillation_dataset/pkl/pkl_255',
            'new_validation_dataset/raw_feature2',
            'new_validation_dataset/targets']

PROCESS_NUM = 100
if __name__ == '__main__':
    import  sys
    if len(sys.argv)<3:
        exit(1)

    extract_clear(tar_list[int(sys.argv[1]):int(sys.argv[2])])
