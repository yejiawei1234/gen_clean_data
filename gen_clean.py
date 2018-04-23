# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
import time
import os
import ast


def gen_opener(filename):
    '''
    Open a sequence of filenames one at a time producing a file object.
    The file is closed immediately when proceeding to the next iteration.
    '''
    f = open(filename, 'r')
    yield f
    f.close()  
        
        
def get_date(filename):
    date = os.path.basename(filename).split('-')[0]
    return date

    
def gen_concatenate(iterators):
    '''
    Chain a sequence of iterators together into a single sequence.
    '''
    for it in iterators:
        yield from it
        
        
def find_id(lines):
    for line in lines:
        t = ast.literal_eval(line)
        if t.get('network_name') == 'appier_int' and t.get('activity_kind') == 'install':
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
            if filename == '.DS_Store':
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


start = time.time()
filelist = find_file('/Users/yeye/Desktop/mffaen')
deal_data(filelist)

end = time.time()
print(f'{end - start}')
