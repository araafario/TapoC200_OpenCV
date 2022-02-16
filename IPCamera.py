import cv2
import numpy as np
import imutils
import math

# Aruco Dictionary
dictionary = cv2.aruco.DICT_4X4_50

vid = cv2.VideoCapture("rtsp://AdminTapo:AdminTapo@125.164.123.174/stream1")
cv2.namedWindow('Show', cv2.WINDOW_FREERATIO)

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

    cv2.imshow("Show", show)

    if cv2.waitKey(1) & 0xFF == ord('q'):
        break

if __name__ == '__main__':
    vid.release()
    cv2.destroyAllWindows()
