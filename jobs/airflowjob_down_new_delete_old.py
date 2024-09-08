from datetime import datetime, timedelta
import os
import subprocess


def down_from_gharchive(target_date_down):
    list_download = []
    for each_time in range(0,24):
        url = f'https://data.gharchive.org/{datetime.strptime(target_date_down, "%Y-%m-%d").strftime("%Y-%m-%d")}-{each_time:2d}.json.gz'
        temp_result = subprocess.run(['wget', '-P', folder, url])
        if temp_result.returncode == 0: # 0 다운성공 / 8 다운실패
            list_download.append(temp_result.args[3])

    return list_download

def del_old_file_gharchive(target_date_delete):
    list_deleted = []
    for each_time in range(0,24):
        file_path = f"{folder}{datetime.strptime(target_date_delete, '%Y-%m-%d').strftime('%Y-%m-%d')}-{each_time}.json.gz"
        if os.path.isfile(file_path):
            os.remove(file_path)
            list_deleted.append(file_path)

    return list_deleted

# 대상 폴더 설정
folder = '../data/gh_archive/' # jupyter경로
os.makedirs(folder, exist_ok=True)

if __name__ == "__main__":
    # Download from GhArchive
    target_date_down = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    target_date_down = "2024-08-29" #for test
    down_from_gharchive(target_date_down)

    # Delete old data
    target_date_delete = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
    del_old_file_gharchive(target_date_delete)

