import gevent
from minio import Minio
from argparse import ArgumentParser
from gevent.lock import BoundedSemaphore
import signal
import time
import math
import os
import csv
import hashlib
import shutil
import uuid
import argparse
import random

lock = BoundedSemaphore(1)


class Utils(object):
    def __init__(self, options):
        self.options = options

    def parse_minios_file(self, minio_file):
        minios = []
        with minio_file:
            reader = csv.reader(minio_file)
            for minio in reader:
                minios.append(minio)
        return minios

    def create_files(self, file_num_size_list):
        """
        file_num_size_list: list of tuples, each tuple has files number and file size
        """
        files_path = '/tmp/minio'
        os.makedirs(files_path)

        def create_job(files_num, file_size):
            file_base = (os.urandom(10) * math.ceil(file_size / 10))[:file_size]
            for f in range(files_num):
                file_data = (file_base + os.urandom(100))[100:]
                file_md5 = hashlib.md5(file_data).hexdigest()
                file_name = '{}_{}'.format(file_size, file_md5)
                file_path = os.path.join(files_path, file_name)
                with open(file_path, 'wb') as f:
                    f.write(file_data)

        cjobs = [gevent.spawn(create_job, file_num_size[0], file_num_size[1]) for file_num_size in file_num_size_list]
        gevent.joinall(cjobs)

        print('finished generating files')
        print('started coping folder containg generated files')
        files_paths = []
        # copy directory
        files_paths.append(files_path)
        for m in range(options.minios_num - 1):
            copy_dir_name = files_path + str(m + 2)
            files_paths.append(copy_dir_name)
            shutil.copytree(files_path, copy_dir_name)
        print("Finished copying folders")
        return files_paths

    def upload_download_files(self, minio_client, files_path):
        """
        files_path: is the path that has the files for one s3
        """
        def job(minio_client, bucket, res_path, file_path):
            """
            file_path: specific path for a file to be uploaded/downloaded
            """

            with workers:
                file_name = file_path.split('/')[-1]
                file_size = int(file_name.split('_')[0])
                file_md5 = file_name.split('_')[-1]
                # upload file
                u_start = time.time()
                minio_client.fput_object(bucket, file_name, file_path)
                u_end = time.time()
                file_upload_speed = (file_size / ((u_end - u_start) * 1024 * 1024))

                # download file
                d_start = time.time()
                d_file = minio_client.get_object(bucket, file_name)
                d_end = time.time()
                assert(hashlib.md5(d_file.data).hexdigest() == file_md5)
                file_download_speed = (file_size / ((d_end - d_start) * 1024 * 1024))

                # results
                data = [file_name, file_size, file_upload_speed, u_start,
                        u_end, file_download_speed, d_start, d_end]
            with lock:
                results_file = open(res_path, 'a')
                with results_file:
                    writer = csv.writer(results_file)
                    writer.writerow(data)

        # create bucket
        bucket_name = str(uuid.uuid4()).replace('-', '')[:10]
        minio_client.make_bucket(bucket_name)

        files_names = os.listdir(files_path)
        files_joint_list = []
        for file_name in files_names:
            files_joint_list.append(os.path.join(files_path, file_name))

        # results file name
        res_file = files_path.split('/')[-1] + '_res.csv'
        res_path = files_path + '/' + res_file
        data = ['File Name', 'File Size (Bytes)', 'Upload Speed (MB/s)', 'U_start_time',
                'U_end_time', 'Download Speed (MB/s)', 'D_start_time', 'D_end_time']
        results_file = open(res_path, 'a')
        with results_file:
            writer = csv.writer(results_file)
            writer.writerow(data)

        random.shuffle(files_joint_list)
        jobs = [gevent.spawn(job, minio_client, bucket_name, res_path, file_path) for file_path in files_joint_list]
        gevent.joinall(jobs)

    # later on get the objects names using the minio client if possible
    def teardown_minios(self, files_names, minio_client):
        buckets = minio_client.list_buckets()
        for bucket in buckets:
            for file_name in files_names:
                minio_client.remove_object(bucket.name, file_name)
            minio_client.remove_bucket(bucket.name)


def main(options):
    utils = Utils(options)

    # clean data folders
    os.system('rm -rf /tmp/minio*')

    # create files for all minios to be uploaded
    def pairs(single):
        iterable = iter(single)
        while True:
            yield next(iterable), next(iterable)
    file_num_size_list = []
    for files_num, file_size in pairs(options.files_num_sizes):
        file_num_size_list.append((int(files_num), int(file_size)))
    files_paths = utils.create_files(file_num_size_list)

    # upload/download files to/from minios servers
    minios = utils.parse_minios_file(options.minios_file)

    # Get minio clients
    minio_clients = []
    for minio in minios:
        minio_url = minio[0]
        minio_key = minio[1]
        minio_secret = minio[2]
        try:
            minio_client = Minio(minio_url, access_key=minio_key,
                                 secret_key=minio_secret, secure=False)
        except:
            pass
        minio_clients.append(minio_client)

    sjobs = [gevent.spawn(utils.upload_download_files, minio_clients[m], files_paths[m]) for m in range(options.minios_num)]
    gevent.joinall(sjobs)

    # teardown
    if options.teardown:
        files_names = os.listdir(files_paths[0])
        tjobs = [gevent.spawn(utils.teardown_minios, files_names, minio_clients[m]) for m in range(options.minios_num)]
        gevent.joinall(tjobs)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-w", "--workers", type=int, default=10, dest="workers_num", required=True,
                        help='Number of greenlets to uploade/download files to/from the minio server simultaneously')
    parser.add_argument("-m", "--minios", type=int, default=5, dest="minios_num",
                        help='Number of minio servers to be used')
    parser.add_argument("-f", "--minios_file", type=argparse.FileType('r'), dest='minios_file', required=True,
                        help="File that contains all minios, each line has minios's url, key and secret")
    parser.add_argument('-p', '--files_num_sizes', dest='files_num_sizes', nargs='*',
                        help="pairs of the number and the size(in Bytes) of files needed .. ex: 10 10000000 20 1000000000: this means 10 files of 10MB and 20 files of 1GB")
    parser.add_argument('--no_teardown', dest='teardown', default=True, action='store_false',
                        help='Remove all files and all buckets for all minios, if "--no_teardown" flag is passed"')

    options = parser.parse_args()
    if len(options.files_num_sizes) % 2:
        parser.error('files_num_sizes arg should be pairs of values')

    workers = BoundedSemaphore(options.workers_num)
    gevent.signal(signal.SIGQUIT, gevent.kill)
    main(options)
