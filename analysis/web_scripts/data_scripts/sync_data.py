import subprocess, time, argparse


import os
PARENT_DIR = os.path.abspath('../../')  # analysis dir
REMOTE_DIR='gs:/nse-data-bucket'
OUT_DIR      = f'{PARENT_DIR}/out'
NSE_DATA_DIR = 'nse_data'
INTRADAY_DIR ='intraday'
INDX_DIR ='index'

NSE_INDX_DATA = f'{PARENT_DIR}/{NSE_DATA_DIR}/{INDX_DIR}'
REMOTE_NSE_INDX_DATA = f'{REMOTE_DIR}/{NSE_DATA_DIR}/{INDX_DIR}'

NSE_INTRADAY_DIR_PATH = f'{PARENT_DIR}/{NSE_DATA_DIR}/{INTRADAY_DIR}'
REMOTE_INTRADAY_DIR_PATH = f'{REMOTE_DIR}/{NSE_DATA_DIR}/{INTRADAY_DIR}'
TEMPLATES_PARENT_DIR = f'{os.path.abspath('../')}/templates_flask'


def sync_data_args(src,dst):
 
    cmd = [
        "rclone",
        "copy",
        src,
        dst,
        "--ignore-existing",
        '--fast-list',
        '--size-only',
        '--log-level=INFO',
        '--stats=0',
        '--exclude',"/.*" ,
        '--exclude', "**/.*"
    ]

    try:
        start = time.time()
        cmdout= subprocess.run(cmd, check=True,stdout=subprocess.PIPE,stderr=subprocess.STDOUT, text=True)
        if cmdout.stdout:
            print(cmdout.stdout)
        
        print(f"✅ Sync from {src} to {dst} completed successfully in {time.time() - start:.2f}s ")
 
    except subprocess.CalledProcessError as e:
        print("❌ rclone failed")
        print(e.stdout)



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='NSE sync data')
    parser.add_argument('-tr', '--to-remote', action='store_true', help='Sync to remote drive')
    parser.add_argument('-ix', '--index', action='store_true', help='Sync to remote drive')



    args, unknown = parser.parse_known_args()
    to_remote = False
    index_flag = False

    if args.to_remote:
        print("Sync to remote drive")
        to_remote = True

    if args.index:
        print("Sync Index Dir")
        index_flag = True

    
    if to_remote:
        if not index_flag:
            sync_data_args(NSE_INTRADAY_DIR_PATH,REMOTE_INTRADAY_DIR_PATH)
        else:
            sync_data_args(NSE_INDX_DATA,REMOTE_NSE_INDX_DATA)

    else:
        sync_data_args(REMOTE_INTRADAY_DIR_PATH,NSE_INTRADAY_DIR_PATH)