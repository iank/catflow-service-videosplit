# Test data

## `car.mp4`

This is a 640x480 video containing 11 frames. Each frame is the same, a CC-licensed photo of a car downloaded from Wikipedia. It was generated using the following:

```
from moviepy.editor import *
img = ImageClip('car.png')
num_frames = 11
fps = 5
duration = num_frames / fps
img = img.set_duration(duration)
img = img.resize((640, 480))
img.write_videofile("car.mp4", fps=fps)
```

and the frame count can be verified with:

```
ffmpeg -i car.mp4 -map 0:v:0 -c copy -f null -
```
