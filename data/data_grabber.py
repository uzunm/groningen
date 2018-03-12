import obspy
import requests
import io
import matplotlib
import pandas as pd
import helpers
import functools
import datetime
import copy

def get_event_response(network=None, station=None, channel=None,
					   eventid=None,starttime=None, endtime=None):
    """get an event mseed file from the knmi database given
    query parameters describing the event"""
    baseurl = 'http://rdsa.knmi.nl/fdsnws/dataselect/1/query'
    payload = {'starttime':starttime,'endtime':endtime,'network':network,
               'station':station, 'channel':channel,'nodata':404}
    r = requests.get(baseurl,params=payload)
    if r.status_code ==200:
    	return {'data':r.content,'starttime':starttime,'endtime':endtime
                ,'channel':channel,'station':station, '_id':eventid}
@helpers.bind
def streamify(event_response):
    data = event_response['data']
    st = obspy.read(io.BytesIO(data))
    useful_keys = ['starttime','endtime','channel','station','_id']
    streamified = {'stream':st}
    for k in useful_keys:
        streamified[k]=event_response[k]
    return streamified
@helpers.bind
def stream_formatter(stream_in,my_start_time,my_dur):
	"""murat fill in here"""

    my_start_time=obspy.core.utcdatetime.UTCDateTime(my_start_time)

    trace_in = stream_in['stream'][0]
    channel_in = stream_in['channel']
    station_in = stream_in['station']
    
    my_end_time = my_start_time + my_dur
    trace_in.trim(my_start_time,my_end_time)
    
    stream_in['stream'][0]=trace_in
    
    stream_in['starttime']=str(obspy.core.utcdatetime.UTCDateTime(trace_in.stats.starttime))
    stream_in['endtime']=str(obspy.core.utcdatetime.UTCDateTime(trace_in.stats.endtime))
       
    return stream_in 
	
def add_to_timestring(nseconds, isostring):
	"""given a timestamp in iso8601 string format, add a timedelta
	of nseconds to the time and return
	a new iso8601 string with the incremented time."""
	currenttime = datetime.datetime.strptime(isostring,"%Y-%m-%dT%H:%M:%S.%f")
	delta = datetime.timedelta(seconds=nseconds)
	futuretime = currenttime+delta
	return datetime.datetime.strftime(futuretime,"%Y-%m-%dT%H:%M:%S.%f")
def parse_events(f='events.psv', incrementer = functools.partial(add_to_timestring,300)):
	df = pd.read_csv(f, delimiter = '|',encoding='utf-8')[['#EventID','Time']]
	return ({'event':{'eventid':eventid,'starttime':time, 'endtime':incrementer(time)}} for index,eventid,time in df.itertuples())

get_and_streamify = helpers.pipe(get_event_response, streamify) #pipe(f,g)() is equivalent to f(g())
get_and_format = helpers.pipe(get_event_response, streamify, stream_formatter)
def unroll(job_d):
	event_d =job_d['event']
	del job_d['event']
	return helpers.merge_dicts(job_d, event_d)
def create_jobs_queue():
	"""create a generator of job parameter dicts like:
	[{'starttime':s,'endtime':e,'network':n,
               'station':s, 'channel':c},,,"""
	n = {'network': ['NL']}
	stations = {'station':['VKB']}
	channels = {'channel':['BHZ']}
	events = helpers.lstdcts2dctlsts(helpers.first_n(parse_events(), 5))
	all_combs = helpers.many_dict_product(n, stations, channels, events)
	return map(unroll,all_combs)

def serial_worker(jobs_queue):
	"""given an iterator of event dictionaries 
	{'starttime':s,'endtime':e,'network':n,
     'station':s, 'channel':c}
	get the http response for that set of parameteres,,
	extract the mseed stream and format it."""
	return (get_and_format(**job) for job in jobs_queue)
def parallel_worker(jobs_queue):
	"""multithreaded concurrent version of serial worker"""
	jobs = (functools.partial(get_and_format, **job) for job in jobs_queue)
	res = helpers.run_chunks_parallel(jobs, chunksize = 10)
	return res
if __name__ == '__main__':
	q = create_jobs_queue()
	res = parallel_worker(q)
	for r in res:
		print(r)