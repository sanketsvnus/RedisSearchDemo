import redis
import logging
import matplotlib.pyplot as plt;
import numpy as np

redis_server = '10.0.0.141'
redis_port = '6379'
# Open handle for redis-server installed at CentOS VM using redis package.
r = redis.Redis(
    host=redis_server,
    port=redis_port)

# Get logger to log proper Info/Debug messages.
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(name)s %(levelname)s %(message)s')
logger = logging.getLogger('Search_Redis')

# Retrieve information from redis to build x_axis and y_axis
logger.debug(r.hkeys('Execuation_Time'))
executionmode = tuple(i.decode("utf-8") for i in r.hkeys('Execuation_Time'))
logger.debug(executionmode)
responsetime = [float(r.hget('Execuation_Time', x).decode("utf-8")) for x in executionmode]
logger.debug(responsetime)

# Prepare xpos to plot bar.
xpos = np.arange(len(executionmode))
logger.debug(xpos)

# Plot bar
plt.bar(xpos, responsetime, label="responsetime")

# Label it properly
plt.xticks(xpos, executionmode)
plt.yticks(responsetime)
plt.ylabel('Response Time in Seconds')
plt.xlabel('SEARCH VS SCAN')
plt.title('REDIS SEARCH BEATS SCAN')
plt.legend()

# Display Text on the top of each BarPlot
for i in xpos:
    plt.text(x=xpos[i] - 0.18, y=responsetime[i] + 0.05, s=responsetime[i], size=6)

# Adjust the margins
plt.subplots_adjust(bottom=0.1, top=0.90)

#plt.show()
plt.savefig('Result_Visualization')

