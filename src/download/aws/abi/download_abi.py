import s3fs
import datetime as dt
import os

base_path = "/home/adriano/eda-goes16"


def get_julian_day(year, month, day):
    if isinstance(year, int): year = str(year)
    if isinstance(month, int): month = str(month).zfill(2)
    if isinstance(day, int): day = str(day).zfill(2)
    
    fmt_date = dt.datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d')
    return str(fmt_date.timetuple().tm_yday).zfill(3)


def download_abi_data(year, month, day, hour, product='ABI-L2-CMIPC'):
    if isinstance(year, int): year = str(year)
    if isinstance(month, int): month = str(month).zfill(2)
    if isinstance(day, int): day = str(day).zfill(2)
    if isinstance(hour, int): hour = str(hour).zfill(2)

    julian_day = get_julian_day(year, month, day)

    fs = s3fs.S3FileSystem(anon=True)

    remote_fps = fs.ls(f's3://noaa-goes16/{product}/{year}/{julian_day}/{hour}')

    total_files = len(remote_fps)
    for i, remote_fp in enumerate(remote_fps):
        file_base_path =  '/'.join(remote_fp.split(os.sep)[:-1])
        path = f'{base_path}/data/{file_base_path}'
        local_fp = f"{path}/{remote_fp.split(os.sep)[-1]}"
        
        if not os.path.exists(path):
            print(f"[INFO] - Creating directory: {path}")
            os.makedirs(path)
        
        if not os.path.exists(local_fp):
            print(f"[INFO] - Downloading {i+1}/{total_files}: {remote_fp}")
            fs.get(remote_fp, local_fp)

download_abi_data(2020, 8, 14, 0)