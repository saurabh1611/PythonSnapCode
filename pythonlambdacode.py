import json
import boto3
import os
from datetime import datetime, timezone
import time

ec2_client = boto3.client('ec2', region_name="us-east-1")

def archive_snapshot(snapshot_id):
    print("Moving Snapshot ID to archive state " + snapshot_id)
    response = ec2_client.modify_snapshot_tier(SnapshotId=snapshot_id, StorageTier='archive')

def fetch_runing_snapshots():
    response = ec2_client.describe_snapshot_tier_status(
            Filters=[
                {
                    'Name': 'last-tiering-operation',
                    'Values': [
                        'archival-in-progress',
                        ]
                }
            ]
    )
    running_snaps = 0
    for snaps in response['SnapshotTierStatuses']:
        if snaps['LastTieringOperationStatus'] == 'archival-in-progress':
            running_snaps = running_snaps + 1

    return running_snaps

def lambda_handler(event, context):
    snapshot_response = ec2_client.describe_snapshots(OwnerIds=['self'])
    Snaps_to_archieve = []
    Backup_policy = ""

    for snapshot in snapshot_response['Snapshots']:
        days_old = (datetime.now(timezone.utc) - snapshot['StartTime']).days

        if days_old < 10:
            if snapshot['Tags']:
                for j in snapshot['Tags']:
                    if j['Key'] == 'CustomBackupScheduleType':
                        Backup_policy = j['Value']

                if Backup_policy == "Monthly":
                    if snapshot['StorageTier'] == "standard":
                        Snaps_to_archieve.append(snapshot['SnapshotId'])

                Backup_policy = ""

    count = 0

    for snap_id in Snaps_to_archieve:
        count = count + 1
        archive_snapshot(snap_id)
        if count % 5 == 0:
            print("Sleeping for 10 minutes, Snapshot archival in process")
            time.sleep(600)
            number_of_running_snaps = fetch_runing_snapshots()
            if number_of_running_snaps != 0:
                print("Sleeping for 10 minutes, Snapshots archival Still in progress")
                time.sleep(600)
