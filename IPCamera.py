import threading
import cv2
import imutils
import time
import json
import rx
from rx.scheduler import ThreadPoolScheduler
from rx import operators as ops
import psycopg2

# calculate cpu count, using which will create a ThreadPoolScheduler

conn = psycopg2.connect(database="testdb", user = "postgres", password = "asd", host = "127.0.0.1", port = "5432")

dictionary = cv2.aruco.DICT_4X4_50

config = json.load(open('config.json'))

thread_count = len(config)
thread_pool_scheduler = ThreadPoolScheduler(thread_count)
print("Cpu count is : {0}".format(thread_count))

i = -1

def startCamDetect(observer, scheduler):
    global i
    i+=1
    id = i
    # vid = cv2.VideoCapture("rtsp://AdminTapo:AdminTapo@125.164.123.174/stream1")
    vid = cv2.VideoCapture(config[i]['url'])
    # cv2.namedWindow('Show', cv2.WINDOW_FREERATIO)
    tempids = []
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
            if(ids is not None):
                flattenedids = ids.flatten()
                if len(tempids) > 0:
                    if tempids != flattenedids:
                        tempids = flattenedids
                        observer.on_next({"id": id, "data": flattenedids.tolist()})
                        # observer.on_next({"id": id, "data": flattenedids.tolist() + [1]})
                else:
                    tempids = flattenedids
                    observer.on_next({"id": id, "data": flattenedids.tolist()})
                    # observer.on_next({"id": id, "data": flattenedids.tolist() + [1]})
            else:
                if(len(tempids) > 0):
                    tempids = []
                    # observer.on_next([])
                    observer.on_next({"id": id, "data": []})


        # cv2.imshow("Show", show)

        # if cv2.waitKey(1) & 0xFF == ord('q'):
        #     observer.on_completed()
        #     vid.release()
        #     # cv2.destroyAllWindows()
        #     break
        
def execToDb(data):
    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS bak_filter (id_bak CHAR(10) PRIMARY KEY NOT NULL, status BOOLEAN NOT NULL);")
    cur.execute(data[0], data[1])
    conn.commit()
    cur.close()

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

for x in range(len(config)):
    rx.create(startCamDetect)\
    .pipe(
        ops.subscribe_on(thread_pool_scheduler)
        )\
    .subscribe(
        lambda i : threading.Thread(target=processData, args=(i,)).start(),
        lambda e: print(e),
        lambda: print("Camera stopped")
        )


print("asd")