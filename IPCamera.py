import threading
import cv2
import imutils
import time
import json
import rx
from rx.scheduler import ThreadPoolScheduler
from rx import operators as ops
import psycopg2

conn = psycopg2.connect(database="testdb", user = "postgres", password = "asd", host = "127.0.0.1", port = "5432")

dictionary = cv2.aruco.DICT_4X4_50

config = json.load(open('config.json'))

thread_count = len(config)
thread_pool_scheduler = ThreadPoolScheduler(thread_count)
print("camera thread count is : {0}".format(thread_count))

def generateCam(x):
    def startCamDetect(observer, scheduler):
        # vid = cv2.VideoCapture("rtsp://AdminTapo:AdminTapo@125.164.123.174/stream1")
        vid = cv2.VideoCapture(config[x]['url'])
        
        while True:
            ret, frame = vid.read()
            image = frame
            imutils.resize(image, width=600)
            show = image
            image = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

            if ret:
                arucoDict = cv2.aruco.Dictionary_get(dictionary)
                arucoParams = cv2.aruco.DetectorParameters_create()
                (corners, ids, rejected) = cv2.aruco.detectMarkers(image, arucoDict, parameters=arucoParams)
                cv2.aruco.drawDetectedMarkers(image=show, corners=corners, ids=ids, borderColor=(0, 255, 0))
                observer.on_next(ids)
    
    tempids = []
    def filterObserver(ids):
        nonlocal tempids
        if(ids is not None):
            if len(tempids) > 0:
                if tempids != ids:
                    tempids = ids
                    return True
            else:
                tempids = ids
                return True
        else:
            if(len(tempids) > 0):
                tempids = []
                return True
        return False
    
    rx.create(startCamDetect)\
    .pipe(
        ops.filter(filterObserver),
        ops.map(lambda i: {"id" : x, "data" : []} if i is None else {"id" : x, "data" : i.flatten().tolist()}),
        ops.subscribe_on(thread_pool_scheduler)
        )\
    .subscribe(
        lambda i : threading.Thread(target=processData, args=(i,)).start(),
        lambda e: print(e),
        lambda: print("Camera stopped")
        )

def processData(data):
    sql = "insert into bak_filter(id_bak,status) values (%s, %s) on conflict (id_bak) do update set status=excluded.status;"
    rx.of(*config[data["id"]]["ids"])\
    .pipe(
        ops.map(lambda i: [i["bakid"], True] if i["arucoid"] in data["data"] else [i["bakid"], False]),
        ops.map(lambda i : [sql, i]),
        ops.reduce(lambda acc, i: [acc[0] + i[0], acc[1] + i[1]]),
        )\
    .subscribe(
        lambda i : execToDb(i),
        lambda e: print(e)
        )

def execToDb(data):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS bak_filter (id_bak CHAR(10) PRIMARY KEY NOT NULL, status BOOLEAN NOT NULL);")
    cur.execute(data[0], data[1])
    conn.commit()
    cur.close()
    print("executed")

for x in range(len(config)):
    generateCam(x)

print("main thread closed")