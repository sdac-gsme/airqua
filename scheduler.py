"""Scheduler
"""
from datetime import datetime
import time

import requests
import schedule
import jdatetime

from src.airnow import Pollution
from src.data_handler import Ckan


RUN_URL = "https://healthchecks.sdac.ir/ping/2fec9b55-c6f5-4cdd-9658-e8d9f5b01909"
DATA_FLOW_URL = "https://healthchecks.sdac.ir/ping/b0795427-d744-4dc4-b8cf-2355e5441843"


def ping(url: str, state: str | None = None, retry: int =3):
    """Ping"""
    if state is not None:
        url += f"/{state}"
    for _ in range(retry):
        try:
            requests.get(url, timeout=100)
            return True
        except requests.exceptions.ReadTimeout:
            time.sleep(10)
    return False


def get_and_upload_data(day: datetime):
    """Get and Upload data"""
    ckan = Ckan()
    day_dict = {
        "year": day.year,
        "month": day.month,
        "day": day.day,
    }
    for _ in range(3):
        try:
            pollution = Pollution()
            pollution.upsert_data(**day_dict)
        except requests.exceptions.ReadTimeout:
            time.sleep(300)
        else:
            ckan.add_recordes_from_database("Pollution", day_dict)
            return True
    return False


def hourly_update():
    """Hourly update
    """
    ping(DATA_FLOW_URL, "start")
    today = jdatetime.datetime.today()
    if today.hour > 0:
        selected_day = today
    else:
        selected_day = today - jdatetime.timedelta(days=1)
    success = get_and_upload_data(selected_day) # type: ignore
    if success:
        ping(DATA_FLOW_URL)
    else:
        ping(DATA_FLOW_URL, "fail")


def daily_update():
    """Daily update
    """
    selected_day = jdatetime.datetime.today() - jdatetime.timedelta(days=1)
    get_and_upload_data(selected_day) # type: ignore


schedule.every().hour.at("10:00").do(hourly_update)
schedule.every().day.at("08:00:00").do(daily_update)


if __name__ == "__main__":
    schedule.run_all()
    while True:
        schedule.run_pending()
        ping(RUN_URL)
        time.sleep(30)
