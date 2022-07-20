import boto3
from datetime import datetime, timezone
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',
)
log = logging.getLogger()

def list_log_groups(group_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
            'logGroupNamePrefix': group_name,
            } if group_name else {}
    res = cwlogs.describe_log_groups(**params)
    return res['logGroups']

def list_log_group_streams(group_name, stream_name=None, region_name=None):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
            'logGroupName': group_name,
            } if group_name else {}
    if stream_name:
        params['logStreamNamePrefix'] = stream_name
    res = cwlogs.describe_log_streams(**params)
    return res['logStreams']

def filter_log_events(
    group_name, filter_pat,
    start=None, stop=None,
    region_name=None
):
    cwlogs = boto3.client('logs', region_name=region_name)
    params = {
        'logGroupName': group_name,
        'filterPattern': filter_pat,
    }
    if start:
        params['startTime'] = start
    if stop:
        params['endTime'] = stop
    res = cwlogs.filter_log_events(**params)
    return res['events']


if __name__ == '__main__':
    list_group = list_log_groups(region_name='ap-southeast-1')
    log.info(f'{list_group}')
    
    list_group_streams = list_log_group_streams('/aws/lambda/logtest-pj', region_name = 'ap-southeast-1')
    log.info(f'{list_group_streams}')
    
    filter_events = filter_log_events('/aws/lambda/logtest-pj', 'START', region_name='ap-southeast-1')
    log.info(f'{filter_events}')
    
    start_ts = int(datetime(2022, 7, 19, 8, 31, tzinfo=timezone.utc).timestamp() * 1000)
    end_ts = int(datetime(2022, 7, 19, 8, 33, tzinfo=timezone.utc).timestamp() * 1000)
    
    filter_log = filter_log_events(
        '/aws/lambda/logtest-pj', 'START',
        start=start_ts, stop=end_ts,
        region_name='ap-southeast-1'
    )
    log.info(f'{filter_log}')
    
