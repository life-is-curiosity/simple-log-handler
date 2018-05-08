import schedule
import os
import time
import re
import datetime
import sys, getopt
import zipfile

# Get file map and Backup file map from specified directory
def get_map(start_path):
    file_map = {}
    backup_map = {}
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            if bool(re.compile(r'.*backup').match(f)) is False:
                file_map[f] = os.path.getsize(os.path.join(dirpath, f))/1024/1024
            else:
                backup_map[f] = os.path.getsize(os.path.join(dirpath, f))/1024/1024
    return file_map, backup_map

# Handle Log behavior 
def operation(path, file_map, backup_map, size=1, limit=1):
    # Get files those have been more than <size> MB
    for n in dict(filter(lambda item: item[1] > size, file_map.items())).items():
        rm_file = ''
        early_date = None
        now_date_time = datetime.datetime.now()
        path_and_name = '{}/{}'.format(path, n[0])
        print("{} -> Start to backup : {} ".format(now_date_time.strftime("%Y-%m-%d %H:%M:%S"), path_and_name))

        try:
            # Get all related backup files 
            backup_dict = dict(filter(lambda item: bool(re.compile(r'{}.*backup'.format(n[0])).match(item[0])), backup_map.items()))

            print('backup_dict :--> {}'.format(backup_dict))

            # Backup the original log file as ZIP file
            print("{} -> Zipping .... :{} ".format(
                now_date_time.strftime("%Y-%m-%d %H:%M:%S"), path_and_name))
            with zipfile.ZipFile('{}.zip'.format('{}/{}.{}.backup'.format(path, n[0], now_date_time.strftime("%Y-%m-%d_%H-%M-%S"))), 'w', zipfile.ZIP_DEFLATED) as backupZip:
                backupZip.write(path_and_name, arcname=path_and_name)
                size = sum(
                    [backupZip.file_size for backupZip in backupZip.filelist])

            # Truncate the original log file
            with open(path_and_name, 'r+') as f:
                f.truncate()
            print("{} -> {} has been truncated".format(
                now_date_time.strftime("%Y-%m-%d %H:%M:%S"), path_and_name))

            # Check if there are more than <limit> backup files
            if len(backup_dict) >= limit:
                for n1 in backup_dict.items():
                    current = datetime.datetime.fromtimestamp(os.stat('{}/{}'.format(path, n1[0])).st_mtime)
                    if early_date is None or current < early_date:
                        early_date = current
                        rm_file = n1[0]

                # Rotate logs -> Remove older log
                try:
                    removedFile = '{}/{}'.format(path, rm_file)
                    os.remove(removedFile)
                    print("{} - Removed redundant log : {}, Size: {}".format(now_date_time.strftime("%Y-%m-%d %H:%M:%S"), removedFile, backup_map.get(rm_file)))
                    del backup_map[rm_file]
                except OSError as e:
                    print('IO Error {}'.format(e))

        except PermissionError as pe:
            print('{} -> The process cannot access the file because it is being used by another process'.format(now_date_time.strftime("%Y-%m-%d %H:%M:%S")))

# Job combination
def job(path, size, limit):
    file_map, backup_map = get_map(path)
    operation(path, file_map, backup_map, size, limit)

# Help and Exception Message
def exception():
    print ('Usage : log_handler.py -p <path> -s <size> -l <limit> -t <minute>')
    sys.exit()

def main(argv):
    size = 0
    limit = 0
    minute = 0
    path = ''
    try:
        opts, args = getopt.getopt(argv,"hp:s:l:t:",["help", "path=","size=","limit=", "minute="])
        if len(opts) != 4:
            exception()
        for opt, arg in opts:
            if opt in ("-h", "--help"):
                exception()
            elif opt in ("-p", "--path"):
                path = arg
            elif opt in ("-s", "--size"):
                size = float(arg)
            elif opt in ("-l", "--limit"):
                limit = float(arg)
            elif opt in ("-t", "--minute"):
                minute = float(arg)
    except getopt.GetoptError:
        exception()
    schedule.every(minute).minutes.do(job, path=path, size=size, limit=limit)
    while 1:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    main(sys.argv[1:])
