import json
from operator import itemgetter, attrgetter, methodcaller

class Clip:
    def __init__(self, video, startTimeOffset, endTimeOffset):
        self.video = video
        self.startTimeOffset = startTimeOffset
        self.endTimeOffset = endTimeOffset


    def __repr__(self):
        return repr((self.video, self.startTimeOffset, self.endTimeOffset))


class JSON_Date:
    """ All work with JSON """
    def __init__(self, json_data_file):
        self.data_obj = None
        with open(json_data_file, "r") as read_file:
            self.data_obj = json.load(read_file)
        self.teacher = None
        self.student = None
        self.bad_zip = False
        self.session_id = self.data_obj["sessionId"]
        for file in self.data_obj["files"]:
            # отбрасываем файлы < 10 сек
            if (int(file["endTimeOffset"]) - int(file["startTimeOffset"]))/1000 < 5:
                print((int(file["endTimeOffset"]) - int(file["startTimeOffset"]))/1000)
                continue
            server_data = json.loads(file["serverData"])
            if server_data['role'] == 'teacher':
                """ это учитель """
                self.teacher_id = int(server_data['user_id'])
                self.teacher = Clip(file["streamId"].lower(), int(file["startTimeOffset"]), int(file["endTimeOffset"]))
            else:
                """ это студент """
                self.student_id = int(server_data['user_id'])
                self.student = Clip(file["streamId"].lower(), int(file["startTimeOffset"]), int(file["endTimeOffset"]))
        if self.student is None or self.teacher is None:
            self.bad_zip = True
            return




