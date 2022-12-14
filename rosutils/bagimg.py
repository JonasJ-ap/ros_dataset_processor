import rosbag
import json
from cv_bridge import CvBridge
import cv2
import numpy as np
import os

font = cv2.FONT_HERSHEY_SIMPLEX
textLocation = (10, 30)
textLocationBelow = (10, 60)
fontScale = 1
fontColor = (0, 0, 255)
thickness = 2
lineType = cv2.LINE_AA


def exportVideo(paths, pathOut, targetTopic, speed, fps, printTimestamp, invertImage, rangeFor16Bit):
    speed = int(speed)
    fps = int(fps)
    paths = paths
    paths = list(filter(lambda path: path.strip() != "", paths))
    print("Exporting video from " + targetTopic + " to " + pathOut)
    print("Input bags: " + str(paths))

    bridge = CvBridge()
    startTime = -1
    video = None
    frameCount = -1

    def formatTime(time, startime):
        return str(round((time - startime) * 1e-9, 3)) + "s"

    for path in paths:
        if path.strip() == "":
            continue
        print("Processing " + path)
        bagIn = rosbag.Bag(path)
        for topic, msg, t in bagIn.read_messages(topics=[targetTopic]):
            if startTime == -1:
                startTime = int(str(t))
                video = cv2.VideoWriter(pathOut, cv2.VideoWriter_fourcc(
                    *"mp4v"), fps, (msg.width, msg.height))
                print("Video dimensions: " +
                      str(msg.width) + "x" + str(msg.height))
                print("Input encoding: ", msg.encoding)

            frameCount += 1
            if frameCount % speed != 0:
                continue

            cv_img = np.array(bridge.imgmsg_to_cv2(msg))
            if "16UC1" in msg.encoding or "mono16" in msg.encoding:
                if rangeFor16Bit is None:
                    minVal = cv_img.min()
                    maxVal = cv_img.max()
                else:
                    minVal = rangeFor16Bit[0]
                    maxVal = rangeFor16Bit[1]

                rangeVal = maxVal - minVal
                cv_img = ((cv_img - minVal) / rangeVal * 256).astype("uint8")
                cv_img = cv2.cvtColor(cv_img, cv2.COLOR_GRAY2RGB)

            if invertImage:
                cv_img = cv2.bitwise_not(cv_img)

            if printTimestamp == "sec":
                cv2.putText(cv_img, formatTime(int(str(t)), startTime),
                            textLocation, font, fontScale, fontColor, thickness, lineType)
            elif printTimestamp == "timestamp":
                cv2.putText(cv_img, str(t), textLocation, font,
                            fontScale, fontColor, thickness, lineType)
            elif printTimestamp == "both":
                cv2.putText(cv_img, formatTime(int(str(t)), startTime),
                            textLocation, font, fontScale, fontColor, thickness, lineType)
                cv2.putText(cv_img, str(t), textLocationBelow, font,
                            fontScale, fontColor, thickness, lineType)

            video.write(cv_img)
            # cv2.imshow("Video preview", cv_img)
            cv2.waitKey(1)
    if video is not None:
        video.release()
    else:
        print("No frames found")
    cv2.destroyAllWindows()
    return {"numFrames": frameCount}
