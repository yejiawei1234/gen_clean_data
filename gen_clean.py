# -*- coding: utf-8 -*-
"""

﻿@author: yejiawei
"""
import time
import os
import ast
import pandas as pd
from multiprocessing import Pool
import click
from functools import partial


def gen_opener(filename):
    """
    Open a sequence of filenames one at a time producing a file object.
    The file is closed immediately when proceeding to the next iteration.
    """
    f = open(filename, 'r')
    yield f
    f.close()  
        
        
def get_date(filename):
    date = os.path.basename(filename).split('-')[0]
    return date

    
def gen_concatenate(iterators):
    """
    Chain a sequence of iterators together into a single sequence.
    """
    for it in iterators:
        yield from it
        
        
def find_id(lines):
    for line in lines:
        t = ast.literal_eval(line)
        if t.get('network_name') == 'network_name' and t.get('activity_kind') == 'install':
            click_time = t.get('click_time')
            installed_at = t.get('installed_at')
            adid = t.get('adid')
            activity_kind = t.get('activity_kind')
            ip_address = t.get('ip_address')
            country = t.get('country')
            language = t.get('language')
            gps_adid = t.get('gps_adid')
            result = {'click_time': click_time,
                      'installed_at': installed_at,
                      'adid': adid,
                      'activity_kind': activity_kind,
                      'ip_address': ip_address,
                      'country': country,
                      'language': language,
                      'gps_adid': gps_adid}
            yield result


def find_file(top):
    file_path_list = []
    for path, dirlist, filelist in os.walk(top):
        for filename in filelist:
            if filename.startswith('.'):
                pass
            else:
                filepath = os.path.join(path, filename)
                file_path_list.append(filepath)
    return file_path_list


def deal_data(filepath):
    for filename in filepath:
        print(filename)
        file = gen_opener(filename)
        lines = gen_concatenate(file)
        id_info = find_id(lines)
        df = pd.DataFrame(id_info)
        date = get_date(filename)
        df['date'] = date
        df.to_excel(f'/Users/yeye/Desktop/output/{date}.xlsx')


def deal_data2(filename, folder):
    print(filename)
    file = gen_opener(filename)
    lines = gen_concatenate(file)
    id_info = find_id(lines)
    df = pd.DataFrame(id_info)
    date = get_date(filename)
    df['date'] = date
    df.to_excel(f'{folder}/{date}.xlsx')


def deal_data_test(filename, folder):
    date = get_date(filename)
    print(f'{folder}/{date}.xlsx')


def input_folder(input):
    if not os.path.isabs(input):
        home_path = os.path.expanduser('~/Desktop')
        input_folder_path = os.path.join(home_path, input)
        if os.path.exists(input_folder_path):
            print(input_folder_path)
            return input_folder_path
        else:
            raise ValueError('This folder is not existed in Desktop folder')
    else:
        if os.path.exists(input):
            print('-------', input)
            return input
        else:
            raise ValueError('This folder is not existed')


def output_folder(output):
    if not os.path.isabs(output):
        home_path = os.path.expanduser('~/Desktop')
        output_folder_path = os.path.join(home_path, output)
        if os.path.exists(output_folder_path):
            print(output_folder_path)
            return output_folder_path
        else:
            print('the output folder is not existed, creating the folder now')
            os.mkdir(output_folder_path)
            return output_folder_path
    else:
        if os.path.exists(output):
            print('-------', output)
            return output
        else:
            print('the output folder is not existed, creating the folder now')
            os.mkdir(output)
            return output


@click.command()
@click.option('--input', default='input', help='The input folder')
@click.option('--output', default='output', help='The output folder')
def main(input, output):
    input_folder_path = input_folder(input)
    output_folder_path = output_folder(output)
    start = time.time()
    filelist = find_file(input_folder_path)
    pool = Pool()
    deal_data_ = partial(deal_data2, folder=output_folder_path)
    pool.map(deal_data_, filelist)
    end = time.time()
    print(f'{end-start}')


if __name__ == '__main__':
    # Finally, this script can use full power of CPU!!!
    main()

