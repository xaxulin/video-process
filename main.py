import sys
import os
import subprocess
from zipfile import *
import argparse
import shutil
import fcntl
import mysql.connector
from jsondata import json_data_struct
import string
import secrets
import json
import logging
import time
import datetime


logging.basicConfig(filename="/home/video/log.log", level=logging.DEBUG)

''' parse command line args '''
parser = argparse.ArgumentParser(description=' Extract video from zip')
parser.add_argument('zip_folder', help='Input zip folder')
parser.add_argument('process_folder', help='Input process folder for operate with video')
parser.add_argument('final_folder', help='Folder for final video')
parser.add_argument('bad_folder', help='Folder for bad video')
parser.add_argument('backup_zip', help='Folder for backup zip')
parser.add_argument('backup_out', help='Folder for backup final videos')

def clear_process_folder(process_folder):
    if os.path.exists(process_folder):
        for root, dirs, files in os.walk(process_folder):
            for file in files:
                path = os.path.join(process_folder, file)
                os.remove(path)
    else:
        os.mkdir(process_folder)


def get_random_string(n):
    alphabet = string.ascii_letters + string.digits
    password = ''.join(secrets.choice(alphabet) for i in range(n))
    return password


def extract_zip(zip_file, process_folder):
    with ZipFile(zip_file, 'r') as cur_zip:
        cur_zip.extractall(process_folder)


def get_one_file(in_folder, ext):
    allfiles = os.listdir(in_folder)
    for in_file in allfiles:
        if in_file.upper().find(ext.upper()) > -1:
            return os.path.join(in_folder, in_file)
    return ''


def get_list_files(in_folder, ext):
    out_files = []
    allfiles = os.listdir(in_folder)
    for in_file in allfiles:
        if in_file.upper().find(ext.upper()) > -1:
            out_files.append(os.path.join(in_folder, in_file))
    return out_files


def set_lower_case_rename_video(in_folder, ext):
    out_files = []
    allfiles = os.listdir(in_folder)
    for in_file in allfiles:
        if in_file.lower().find(ext.lower()) > -1:
            os.rename(os.path.join(in_folder, in_file), os.path.join(in_folder, in_file[:-5].lower() + ext))


def process_with_json(in_json_file, process_folder, final_folder, final_name):
    """ prepare all data"""
    json_data = json_data_struct.JSON_Date(in_json_file)
    if not json_data.bad_zip:
        create_final_video(process_folder, final_folder, final_name, json_data)
        if os.path.exists(os.path.join(final_folder, final_name)):
            # get images
            w, h, duration = get_width_heght_duration_from_video(os.path.join(final_folder, final_name))
            get_image_from_video(os.path.join(final_folder, final_name), w, h, os.path.join(final_folder, final_name[:-4] + '.jpg'))
            get_image_from_video(os.path.join(final_folder, final_name), int(w/4), int(h/4), os.path.join(final_folder, final_name[:-4] + '_min.jpg'))
            # copy to s3
            copy_to_s3(os.path.join(final_folder, final_name), json_data.session_id)
            copy_to_s3(os.path.join(final_folder, final_name[:-4] + '.jpg'), json_data.session_id)
            copy_to_s3(os.path.join(final_folder, final_name[:-4] + '_min.jpg'), json_data.session_id)
            # mysql add
            url = 'https://dj9gbewusvbbl.cloudfront.net/lesson_rec/{0}/{1}'.format(json_data.session_id, final_name[:-4])
            add_mysql_record(json_data.session_id, json_data.student_id, json_data.teacher_id, url, int(float(duration)))
            return True
        else:
            return False
    else:
        return False


def process_with_zip_file(zip_file, process_folder, final_folder, final_name):
    '''process with zip'''
    extract_zip(zip_file, process_folder)
    json_file = get_one_file(process_folder, ".json")
    set_lower_case_rename_video(process_folder, '.webm')
    if len(json_file) > 0:
        rez = process_with_json(json_file, process_folder, final_folder, final_name)
        return rez
    else:
        return False


def add_mysql_record(sessionId, student_id, teacher_id, link, duration):
    #pass
    #"""
    conn = mysql.connector.connect(
        host="",
        port=3306,
        user="",
        password="",
        database=""
    )
    cur = conn.cursor()
    sql_command = 'INSERT INTO recorded_videos (session_id, student_id, teacher_id, link, duration) VALUES ("{0}", {1}, {2}, "{3}", {4})'.format(sessionId, student_id, teacher_id, link, duration)
    print(sql_command)
    try:
        cur.execute(sql_command)
    except Exception:
        pass
        #print(f"Error: {e}")
    conn.commit()
    #"""


def copy_to_s3(video, sessionId):
    s3_command = 's3cmd put {0} s3://sronline/lesson_rec/{1}/'.format(video, sessionId)
    print(s3_command)
    proc = subprocess.Popen(s3_command, shell=True, stdout=subprocess.PIPE)
    out = proc.stdout.readlines()


def process_with_zip_folder(zip_folder, process_folder, final_folder, bad_folder, backup_zip, backup_out):
    """ find zip in zip_folder and start process """
    allfiles = os.listdir(zip_folder)
    for in_file in allfiles:
        if in_file.upper().find(".ZIP") > -1:
            rand_name = get_random_string(20)
            final_name = rand_name + '.mp4'
            process_folder_one = os.path.join(process_folder, rand_name)
            os.mkdir(process_folder_one)
            if process_with_zip_file(os.path.join(zip_folder, in_file), process_folder_one, final_folder, final_name):
                logging.debug(" {0} Success coding {1}".format(time.ctime(), final_name))
                ''' конвертация прошла успешно '''
                # backup zip
                today = datetime.datetime.today()
                in_file_with_date = "{0}_{1}.zip".format(in_file[:-4], today.strftime("%Y_%m_%d_%H_%M_%S"))
                os.rename(os.path.join(zip_folder, in_file), os.path.join(backup_zip, in_file_with_date))
                # backup final
                shutil.copyfile(os.path.join(final_folder, final_name), os.path.join(backup_out, final_name))
                # чистим папки
                shutil.rmtree(process_folder_one)
            else:
                logging.debug(" {0} Error coding {1}".format(time.ctime(), in_file))
                ''' ошибка - перенос zip в bad'''
                os.rename(os.path.join(zip_folder, in_file), os.path.join(bad_folder, in_file))
    clear_process_folder(final_folder)



def get_image_from_video(video, width, height, image_name):
    """ get image from video"""
    final_command = 'ffmpeg -y  -hide_banner -ss 1 -i {0} -frames:v 1 -s {1}x{2} {3}'.format(video, width, height, image_name)
    print(final_command)
    proc = subprocess.Popen(final_command, shell=True, stdout=subprocess.PIPE)
    out = proc.stdout.readlines()



def get_width_heght_duration_from_video(video):
    """ get h, w , duration from video"""
    final_command = 'ffprobe -v quiet -print_format json -show_streams -i {0}'.format(video)
    print(final_command)
    proc = subprocess.Popen(final_command, shell=True, stdout=subprocess.PIPE)
    out = proc.stdout.readlines()

    streams_str = ''
    for line in out:
        streams_str += line.decode('UTF-8')
    streams_obj = json.loads(streams_str)
    stream_v = streams_obj["streams"][0]
    return stream_v["width"], stream_v["height"], stream_v["duration"]

def create_final_video(process_folder, final_folder, final_name, json_data):
    ss_student = ''
    ss_teacher = ''
    if json_data.teacher.startTimeOffset > json_data.student.startTimeOffset:
        ss_teacher = '-ss ' + str((json_data.teacher.startTimeOffset - json_data.student.startTimeOffset)/1000)
    else:
        ss_student = '-ss ' + str((json_data.student.startTimeOffset - json_data.teacher.startTimeOffset)/1000)
    final_command = 'ffmpeg -y  -hide_banner {3} -c:a libopus -i {1} {4} -c:a libopus -i {0}  -filter_complex "[0][1]scale2ref=\'oh*mdar\':\'if(lt(main_h,ih),ih,main_h)\'[0s][1s];[1s][0s]scale2ref=\'oh*mdar\':\'if(lt(main_h,ih),ih,main_h)\'[1s][0s];[0s][1s]hstack=inputs=2:shortest=1,setsar=1[vp];[vp]pad=ceil(iw/2)*2:ceil(ih/2)*2[v];[0:a]aresample=async=1000[0sync];[1:a]aresample=async=1000[1sync];[0sync][1sync]amix[a]" -map "[v]" -map "[a]" -ac 2 -r 30 -crf 23 {2}'.format(os.path.join(process_folder, json_data.teacher.video + '.webm'), os.path.join(process_folder, json_data.student.video + '.webm'), os.path.join(final_folder, final_name), ss_teacher, ss_student)

    print(final_command)
    proc = subprocess.Popen(final_command, shell=True, stdout=subprocess.PIPE)
    out = proc.stdout.readlines()

''' begin '''
if __name__ == '__main__':
    fp = open(os.path.realpath(__file__), 'r')

    try:
        fcntl.flock(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        print('Other main.py still working ...')
        sys.exit(0)
    args = parser.parse_args()
    """" Check input args """
    in_folder_with_zip = args.zip_folder
    in_folder_process = args.process_folder
    in_folder_final = args.final_folder
    in_bad_folder = args.bad_folder
    backup_zip = args.backup_zip
    backup_out = args.backup_out
    """ Process with zip folder """
    process_with_zip_folder(in_folder_with_zip, in_folder_process, in_folder_final, in_bad_folder, backup_zip, backup_out)
    hour = int(datetime.datetime.today().strftime("%H"))
    if  hour >= 19 or hour <= 3:
        print('Shutdown system')
        subprocess.Popen('sudo shutdown -h now', shell=True, stdout=subprocess.PIPE)


