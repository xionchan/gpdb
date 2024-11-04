import os
import subprocess
import sys 
import boto3
import yaml 
import sqlite3
import time 
from datetime import datetime

dbname = None
del_timestamp = None
update_timestamp = []

# 执行备份
def backup_db():
    backup_command = 'gpbackup --plugin-config /home/gpadmin/scripts/' + dbname + '.yaml --dbname ' + dbname + ' --leaf-partition-data --incremental --jobs 8 --single-backup-dir --without-globals'
    result = subprocess.run(backup_command, shell=True, capture_output=True, text=True)
    
    # 检查命令执行结果
    if result.returncode == 0:
        return 0
    else:
        return 1

# 删除备份， 直接操作源数据库和删除s3目录
def delete_backup():
    global del_timestamp, update_timestamp
    # 删除数据库记录
    cn_data_dir = os.getenv('COORDINATOR_DATA_DIRECTORY')
    conn = sqlite3.connect(cn_data_dir+'/gpbackup_history.db')
    with conn:
        cur = conn.cursor()
        cur.execute(f"select min(timestamp) from backups where date_deleted = '' and database_name = '{dbname}'")
        del_timestamp = cur.fetchone()[0]
        current_time = datetime.now().strftime("%Y%m%d%H%M%S")
        cur.execute(f"update backups set date_deleted = '{current_time}' where timestamp = '{del_timestamp}' and database_name = '{dbname}'")
        cur.execute(f"delete from restore_plans where timestamp = '{del_timestamp}'")
        cur.execute(f"select timestamp from backups where date_deleted = '' and database_name = '{dbname}' order by timestamp")
        update_timestamp = [ result[0] for result in cur.fetchall() ]
        cur.close()
    
    # 删除对象存储文件目录
    # 处理s3yaml文件, 获取s3信息
    s3yaml = "/home/gpadmin/scripts/" + dbname + ".yaml"
    with open(s3yaml, 'r') as s3yamlfile:
        s3conf = yaml.safe_load(s3yamlfile)
    
    s3 = boto3.client('s3',
                      aws_access_key_id=s3conf['options']['aws_access_key_id'],
                      aws_secret_access_key=s3conf['options']['aws_secret_access_key'],
                      endpoint_url=s3conf['options']['endpoint'])
    dir_name = s3conf['options']['folder'] + '/backups/' + del_timestamp[0:8] + '/' + del_timestamp
    # 目录下的所有文件需要依次删除
    response = s3.list_objects_v2(Bucket=s3conf['options']['bucket'], Prefix=dir_name)
    if 'Contents' in response:
        for obj in response['Contents']:
            s3.delete_object(Bucket=s3conf['options']['bucket'], Key=obj['Key'])


# 处理config文件
def handle_config():
    # 处理s3yaml文件, 获取s3信息
    s3yaml = "/home/gpadmin/scripts/" + dbname + ".yaml"
    with open(s3yaml, 'r') as s3yamlfile:
        s3conf = yaml.safe_load(s3yamlfile)
    
    s3 = boto3.client('s3',
                      aws_access_key_id=s3conf['options']['aws_access_key_id'],
                      aws_secret_access_key=s3conf['options']['aws_secret_access_key'],
                      endpoint_url=s3conf['options']['endpoint'])
    
    for timestamp in update_timestamp:
        file_name = s3conf['options']['folder'] + '/backups/' + timestamp[0:8] + '/' + timestamp + '/gpbackup_' + timestamp + '_config.yaml'
        response = s3.get_object(Bucket=s3conf['options']['bucket'], Key=file_name)
        confdata = yaml.safe_load(response['Body'].read().decode('utf-8'))
        
        # 删除confdata['restoreplan'], 写入临时yaml
        restoreplan_list = confdata['restoreplan']
        del confdata['restoreplan']
        with open('/tmp/gpbackup_' + del_timestamp + '_config.yaml', 'w') as tempyaml:
            yaml.safe_dump(confdata, tempyaml)
            tempyaml.write("restoreplan:\n")
            for restoreplan in restoreplan_list:
                if restoreplan['timestamp'] != del_timestamp:
                    for k, v in restoreplan.items():
                        if k == 'timestamp':        
                            tempyaml.write('- timestamp: ' + '"' + v + '"\n')
                        else:
                            if len(v) == 0:
                                tempyaml.write("  tablefqns: []\n")
                            else:
                                tempyaml.write("  tablefqns:\n")
                                for table in v:
                                    tempyaml.write('  - ' + table + '\n')
        
        # 上传到s3
        s3.upload_file('/tmp/gpbackup_' + del_timestamp + '_config.yaml', s3conf['options']['bucket'], file_name)

def main():
    global dbname
    dbname = sys.argv[1].lower()
    bkflag = backup_db()
    if bkflag == 1:
        print('备份异常')
        sys.exit(1)
    
    time.sleep(5)
    delete_backup()
    time.sleep(5)
    handle_config()

if __name__ == "__main__":
    main()
