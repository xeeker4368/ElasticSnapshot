#/usr/bin/python3

from elasticsearch import Elasticsearch
import pytz
from datetime import datetime
from beautifultable import BeautifulTable

elastic_repository = input("What is your repository name? ")

es_database = Elasticsearch(['http://10.10.10.128:9200'],verify_certs=True)

es_snapshots = es_database.snapshot.get(repository=elastic_repository, snapshot='_all')

number_of_snapshots = es_snapshots['snapshots'].__len__()

es_table = BeautifulTable()
# number_of_snapshots = 10
es_table.width_exceed_policy = BeautifulTable.WEP_STRIP
es_table.max_table_width = 300

snapshot_number = 0
es_snapshots_status = ""
es_snapshot_size, es_snapshot_state, es_start_time_aware_est, es_end_time_aware_est, \
    es_snapshot_duration, es_snapshot_start_time, es_snapshot_end_time, es_start_time_unaware_est,\
    es_end_time_unaware_est = {}, {}, {}, {}, {}, {}, {}, {}, {}

es_table.column_headers = ['Snapshot Name', 'Snapshot Size in MB', 'Snapshot Status', 'Snapshot Start Time', 'Snapshot End Time', 'Run Time']

while snapshot_number < number_of_snapshots:
    timezone = pytz.timezone('US/Eastern')
    snapshot_name = es_snapshots['snapshots'][snapshot_number]['snapshot']
    es_snapshots_status = es_database.snapshot.status(repository=elastic_repository, snapshot=snapshot_name)
#    #es_snapshots_status['snapshots'][snapshot_number]
    es_snapshot_size[snapshot_number] = es_snapshots_status['snapshots'][0]['stats']['total_size_in_bytes']
    es_snapshot_size[snapshot_number] = es_snapshot_size[snapshot_number] * 0.000000954
    es_snapshot_state[snapshot_number] = es_snapshots['snapshots'][snapshot_number]['state']
    es_snapshot_start_time[snapshot_number] = es_snapshots['snapshots'][snapshot_number]['start_time']
    es_snapshot_end_time[snapshot_number] = es_snapshots['snapshots'][snapshot_number]['end_time']

#    #Attempt to convert to local time
    es_start_time_unaware_est[snapshot_number] = datetime.strptime(es_snapshot_start_time[snapshot_number], "%Y-%m-%dT%H:%M:%S.%fZ")
#    #print(es_start_time_unaware_est)
    es_start_time_aware_est[snapshot_number] = pytz.timezone('US/Eastern').localize(es_start_time_unaware_est[snapshot_number], is_dst=None)
#    #print(es_start_time_aware_est)
    es_end_time_unaware_est[snapshot_number] = datetime.strptime(es_snapshot_end_time[snapshot_number], "%Y-%m-%dT%H:%M:%S.%fZ")
#    #print(es_end_time_unaware_est)
    es_end_time_aware_est[snapshot_number] = pytz.timezone('US/Eastern').localize(es_end_time_unaware_est[snapshot_number], is_dst=None)
#    #print(es_end_time_aware_est)
#    #es_snapshot_end_time = timezone.localize(es_snapshots['snapshots'][snapshot_number]['end_time'])
    es_snapshot_duration[snapshot_number] = (es_snapshots['snapshots'][snapshot_number]['duration_in_millis']/60000)
#   es_snapshot_duration = es_snapshot_duration/60000

    es_table.append_row([snapshot_name, es_snapshot_size[snapshot_number],
                         es_snapshot_state[snapshot_number], es_start_time_aware_est[snapshot_number],
                         es_end_time_aware_est[snapshot_number], es_snapshot_duration[snapshot_number]])

    snapshot_number = snapshot_number + 1
es_table.sort('Snapshot Name')
print(es_table)

