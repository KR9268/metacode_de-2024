from datetime import datetime, timedelta
import os
import subprocess

def format_dict(dictionary):
    formatted_str = "\n".join([f"* {key}:\n {value}" for key, value in dictionary.items()])
    return formatted_str

def down_from_gharchive(target_date_down, target_path):
    print(f'Recevied target_date_down : {target_date_down}')
    list_path_download = {'Already_exist':[],'Downloaded':[]}
    for each_time in range(0,24):
        file_name = f'{datetime.strptime(target_date_down, "%Y-%m-%d").strftime("%Y-%m-%d")}-{each_time:2d}.json.gz'
        file_path = f"{target_path}{file_name}"
        if os.path.isfile(file_path):
            list_path_download['Already_exist'].append(file_path)
        else:
            url = f'https://data.gharchive.org/{file_name}'
            temp_result = subprocess.run(['wget', '-P', target_path, url])
            if temp_result.returncode == 0: # 0 다운성공 / 8 다운실패
                # list_path_download['Downloaded'](temp_result.args[3]) # url+filename 형태
                list_path_download['Downloaded'].append(file_path)

    return format_dict(list_path_download)

def del_old_file_gharchive(target_date_delete, target_path):
    print(f'Recevied target_date_delete : {target_date_delete}')
    list_deleted = {'Deleted':[]}
    for each_time in range(0,24):
        file_path = f"{target_path}{datetime.strptime(target_date_delete, '%Y-%m-%d').strftime('%Y-%m-%d')}-{each_time}.json.gz"
        if os.path.isfile(file_path):
            os.remove(file_path)
            list_deleted['Deleted'].append(file_path)

    return format_dict(list_deleted)

if __name__ == "__main__":
    # 대상 폴더 설정
    target_path = '../data/gh_archive/' # jupyter경로
    os.makedirs(target_path, exist_ok=True)

    # Download from GhArchive
    target_date_down = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    target_date_down = "2024-08-29" #for test
    down_from_gharchive(target_date_down)

    # Delete old data
    target_date_delete = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    del_old_file_gharchive(target_date_delete)

