import os, sys, re, time, yaml, subprocess
import boto3, psycopg2

# 定义全局参数
DBNAME = None 
S3_FILE = None                     # s3配置文件路径
S3_CONFIG = None                   # 读取s3配置文件的内容
RESTORE_TIMESTAMP = None           # 增量还原的时间戳
CURRENT_TIMESTAMP = None           # 已经还原的最后一个时间戳

# 获取本次还原的时间戳
def get_restore_time():
    # 获取本次恢复的时间戳 : 当前还原时间戳的下一个时间戳
    # 1. 获取当前还原时间戳的日期目录
    global CURRENT_TIMESTAMP, RESTORE_TIMESTAMP
    current_date = CURRENT_TIMESTAMP[0:8]
    backup_status = None 
    
    s3_client = boto3.client('s3',
                             aws_access_key_id = S3_CONFIG['options']['aws_access_key_id'],
                             aws_secret_access_key = S3_CONFIG['options']['aws_secret_access_key'],
                             endpoint_url = S3_CONFIG['options']['endpoint'])
    buck_name = S3_CONFIG['options']['bucket']
    file_path = S3_CONFIG['options']['folder']
    
    # 2. 查看日期目录下是否还有更新的文件
    curr_restore_datedir = file_path + '/backups/' + current_date + '/'
    response = s3_client.list_objects_v2(Bucket=buck_name, Prefix=curr_restore_datedir, Delimiter='/')
    curr_restore_timedirs =  [int(prefix['Prefix'][-15:-1]) for prefix in response['CommonPrefixes']]
    curr_restore_timedirs.sort()
    
    for time_dir in curr_restore_timedirs:
        if time_dir > int(CURRENT_TIMESTAMP):
            # 判断该目录下文件gpbackup_{time_dir}_report中的backup status = 'Success'
            report_file = s3_client.get_object(Bucket=buck_name, Key=curr_restore_datedir + str(time_dir) + '/gpbackup_' + str(time_dir) + '_report')['Body'].read().decode('utf-8')
            for line in report_file.split('\n'):
                if line.startswith('backup status:'):
                    backup_status = line.split(':')[1].strip()
                    break
                
            # 如果状态为None, 那么表示正在备份, 程序退出，并发出告警
            if backup_status == None:
                return 1
                
            # 如果状态不是Success, 那么表示备份失败, 程序退出，并发出告警
            if backup_status != 'Success':
                return 2
            else:
                RESTORE_TIMESTAMP = str(time_dir)
                return 0
    
    # 3. 如果当面日期下没有更新的目录，下面寻找下一个日期
    if RESTORE_TIMESTAMP == None:
        curr_restore_datedir = file_path + '/backups/'
        response = s3_client.list_objects_v2(Bucket=buck_name, Prefix=curr_restore_datedir, Delimiter='/')
        curr_restore_datedirs =  [int(prefix['Prefix'][-9:-1]) for prefix in response['CommonPrefixes']]
        curr_restore_datedirs.sort()

        for date_dir in curr_restore_datedirs:
            if date_dir > int(current_date):
                # 获取该目录下的所有子备份目录
                response = s3_client.list_objects_v2(Bucket=buck_name, Prefix=curr_restore_datedir + str(date_dir) + '/', Delimiter='/')
                time_dirs =  [int(prefix['Prefix'][-15:-1]) for prefix in response['CommonPrefixes']]
                time_dirs.sort()
                
                for time_dir in time_dirs:
                    report_file_name = curr_restore_datedir + str(date_dir) + '/' + str(time_dir) + '/gpbackup_' + str(time_dir) + '_report'
                    s3_client.head_object(Bucket=buck_name, Key=report_file_name)
                    report_file = s3_client.get_object(Bucket=buck_name, Key=report_file_name)['Body'].read().decode('utf-8')
                    for line in report_file.split('\n'):
                        if line.startswith('backup status:'):
                            backup_status = line.split(':')[1].strip()
                            break 
                            
                    # 如果状态为None, 那么表示正在备份, 程序退出，并发出告警
                    if backup_status == None:
                        return 1

                    # 如果状态不是Success, 那么表示备份失败, 程序退出，并发出告警
                    if backup_status != 'Success':
                        return 2
                    else:
                        RESTORE_TIMESTAMP = str(time_dir)
                        return 0
        
    # 4. 如果还原时间为空，那么表示全部备份都已经被恢复了。
    if RESTORE_TIMESTAMP == None:
        return 3

# 获取最新已经还原的备份的时间戳
def get_restore_donetime():
    # 获取已经完成最新恢复的时间戳
    global CURRENT_TIMESTAMP
    reportdir = '/home/gpadmin/scripts/gprestore/' + DBNAME + '/dataonly/-1/backups/'
    # 获取最新完成的备份时间戳： 依次逆序遍历日期目录(YYYYMMDD), 时间目录(YYYYMMDDHH24:MI:SS), 获取最后含有gprestore_{backup_time}_{restore_time}_report的目录，且内容中database name为 s3_config['options']['folder']
    restore_date_dirs = [int(date_dir) for date_dir in os.listdir(reportdir)]
    restore_date_dirs.sort(reverse=True)

    # 从最大日期开始遍历
    for restore_date in restore_date_dirs:
        restore_date_dir = reportdir + str(restore_date) + '/'
        restore_time_dirs = [int(time_dir) for time_dir in os.listdir(restore_date_dir)]
        restore_time_dirs.sort(reverse=True)

        # 遍历最大日期下的时间目录
        for restore_time in restore_time_dirs:
            restore_time_dir = restore_date_dir + str(restore_time) + '/'

            # 遍历目录下的文件，匹配是否满足条件
            file_re = r'^gprestore_\d+_\d+_report$'
            for file in os.listdir(restore_time_dir):
                if re.match(file_re, file):
                    # 读取文件的内容, 判断上一次还原是否成功
                    with open(restore_time_dir + file, 'r') as restore_rpt:
                        for line in restore_rpt:
                            if line.startswith('restore status:'):
                                restore_status = line.split(':')[-1].strip()
                                break 
                        
                        if restore_status == 'Success':
                            CURRENT_TIMESTAMP = str(restore_time)
                            return 0
                        else:  # 返回上一次还原失败
                            return 1 
    return 1

# 还原源数据(表结构)
def metadata_restore():
    reportdir = '/home/gpadmin/scripts/gprestore/' + DBNAME + '/metadata/'
    cmd_logfile = '/home/gpadmin/scripts/gprestore/' + DBNAME + '_metadata_' + RESTORE_TIMESTAMP + '.log'
    restore_cmd = [
        'gprestore',
        '--plugin-config', S3_FILE,
        '--timestamp', RESTORE_TIMESTAMP,
        '--metadata-only',
        '--on-error-continue',
        '--verbose',
        '--report-dir', reportdir
    ]
    
    with open(cmd_logfile, 'a') as file:
        subprocess.run(restore_cmd, stdout=file, text=True)
    time.sleep(5)

# 还原数据
def dataonly_restore():
    reportdir = '/home/gpadmin/scripts/gprestore/' + DBNAME + '/dataonly/' 
    cmd_logfile = '/home/gpadmin/scripts/gprestore/' + DBNAME + '_dataonly_' + RESTORE_TIMESTAMP + '.log'
    restore_cmd = [
        'gprestore',
        '--plugin-config', S3_FILE,
        '--timestamp', RESTORE_TIMESTAMP,
        '--data-only',
        '--incremental',
        '--verbose',
        '--jobs', 8,
        '--report-dir', reportdir
    ]
     
    with open(cmd_logfile, 'a') as file:
        result = subprocess.run(restore_cmd, stdout=file, text=True)
    time.sleep(5)
    return result.returncode

# 删除所有的HEAP表及关联对象
def del_heaptab():
    try:
        cur = db_conn.cursor()
        # 删除HEAP表 : 分区子表以及非分区表
        get_del_heap_r_sql = "select 'drop table '||autnspname||'.'||autrelname||' cascade' from gp_toolkit.__gp_user_tables where autrelam = 2 and autrelkind <> 'p' and autnspname <> 'hint_plan'"
        cur.execute(get_del_heap_r_sql)
        del_hr_sqls = [sql[0] for sql in cur.fetchall()]
        for del_hr_sql in del_hr_sqls:
            cur.execute(del_hr_sql)

        # 删除空分区父表, 没有子表的
        get_del_heap_p_sql = '''select 'drop table '||autnspname||'.'||autrelname||' cascade' from gp_toolkit.__gp_user_tables 
                         where autrelkind = 'p'  and 
                         (autnspname,autrelname) not in (select schemaname,tablename from gp_toolkit.gp_partitions)'''
        cur.execute(get_del_heap_p_sql)
        del_hp_sqls = [sql[0] for sql in cur.fetchall()]
        for del_hp_sql in del_hp_sqls:
            cur.execute(del_hp_sql)
    except psycopg2.errors.UndefinedTable:
        pass
    except Exception as e:
        return e
    finally:
        cur.close()
    
    return None 

# 删除的AO表名字及关联对象
def del_aotab():
    # 获取需要删除哪些ao表
    ao_list = []
    # curr_backup_time = common.get_restore_time()
    # last_retore_time = common.get_restore_donetime()

    # 获取备份里面的ao表信息
    backup_toc_file =  S3_CONFIG['options']['folder'] + '/backups/' + RESTORE_TIMESTAMP[0:8] + '/' + RESTORE_TIMESTAMP + '/gpbackup_' + RESTORE_TIMESTAMP + '_toc.yaml'
    s3_client = boto3.client('s3',
                             aws_access_key_id=S3_CONFIG['options']['aws_access_key_id'],
                             aws_secret_access_key=S3_CONFIG['options']['aws_secret_access_key'],
                             endpoint_url=S3_CONFIG['options']['endpoint'])
    s3_object = s3_client.get_object(Bucket=S3_CONFIG['options']['bucket'], Key=backup_toc_file)
    backup_config = yaml.safe_load(s3_object['Body'].read().decode('utf-8'))['incrementalmetadata']['ao']

    # 获取还原里面的ao表信息
    restore_toc_file = os.getenv('COORDINATOR_DATA_DIRECTORY') + '/backups/' + CURRENT_TIMESTAMP[0:8] + '/' + CURRENT_TIMESTAMP + '/gpbackup_' + CURRENT_TIMESTAMP + '_toc.yaml'
    with open(restore_toc_file, 'r') as f:
        restore_config_file = yaml.safe_load(f)
        restore_config = restore_config_file['incrementalmetadata']['ao']

    # backup中没有的表, restore中有的表, 表示表被删除了
    drop_ao_tab = restore_config.keys() - backup_config.keys()
    ao_list.extend(drop_ao_tab)

    # 同名的表，lastddltime不同或者modcount不同的表
    common_ao_tab = restore_config.keys() & backup_config.keys()
    for ao_tab in common_ao_tab:
        if not restore_config[ao_tab] == backup_config[ao_tab]:
            ao_list.append(ao_tab)

    # 删除ao表
    try:
        cur = db_conn.cursor()
        for ao_tab in ao_list:
            del_ao_sql = 'drop table ' + ao_tab + ' cascade'
            cur.execute(del_ao_sql)
    except psycopg2.errors.UndefinedTable:
        pass
    except Exception as e:
        return e
    finally:
        cur.close()
    
    return None 

# 还原前的处理
def prerestore():
    # 创建数据库连接
    global db_conn
    db_conn = psycopg2.connect(dbname = DBNAME, host = 'localhost')
    db_conn.autocommit = True

    # 删除ao表相关
    delaomsg = del_aotab()
    if delaomsg is not None:
        return delaomsg
    
    # 删除heap表相关    
    delheapmsg = del_heaptab()
    if delheapmsg is not None:
        return delheapmsg
        
    db_conn.close()
    return None

def main():
    global DBNAME, S3_FILE, S3_CONFIG
    DBNAME = sys.argv[1].lower()
    S3_FILE = '/home/gpadmin/scripts/gpbackups3_' + DBNAME + '.yaml'
    
    with open(S3_FILE, 'r') as s3configf:
        S3_CONFIG = yaml.safe_load(s3configf)
    
    while True:
        # 获取上一次成功的还原时间
        check_restore_flag = get_restore_donetime()
        if check_restore_flag == 1:
            print('上一次还原失败')           # zabbix输出 ：上一次还原操作失败
            sys.exit(1)
    
        # 获取本次还原的时间
        get_current_flag = get_restore_time()
        if get_current_flag == 1:
            print('正在进行备份')             # zabbix输出 ：正在进行备份
            sys.exit(1)
        
        if get_current_flag == 2:
            print('最近一次备份状态为失败')    # zabbix输出 ：最新备份状态异常
            sys.exit(1)
        
        if get_current_flag == 3:
            print('还原成功')                 # zabbix输出 ：所有备份还原成功    
            sys.exit(0)
        
        if CURRENT_TIMESTAMP == RESTORE_TIMESTAMP:
            print('还原成功')                 # zabbix输出 ：所有备份还原成功    
            sys.exit(0)
        
        
        # 1. prerestore
        prestatus = None 
        prestatus = prerestore()
        if prestatus is not None:
            print(f'prerestore 失败 {prestatus}')     # zabbix输出 ： 还原前的准备操作失败
            sys.exit(1)
        
        # 2. 还原元数据
        metadata_restore()
        
        # 3. 还原数据
        restore_status = dataonly_restore()
        if restore_status != 0 :
            print('还原失败')                     # zabbix输出 ： 还原失败
            sys.exit(1)

if __name__ == '__main__':
    main()
