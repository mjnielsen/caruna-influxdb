import argparse, json, pycaruna
from datetime import date, datetime, timezone, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from decouple import config


def main():
    CARUNA_CUSTOMER_NUM = config('CARUNA_CUSTOMER_NUM')
    CARUNA_METERING_POINT_NUM = config('CARUNA_METERING_POINT_NUM')
    CARUNA_USERNAME = config('CARUNA_USERNAME')
    CARUNA_PASSWORD = config('CARUNA_PASSWORD')
    INFLUX_URL = config('INFLUX_URL')
    INFLUX_TOKEN = config('INFLUX_TOKEN')
    INFLUX_BUCKET = config('INFLUX_BUCKET')
    INFLUX_ORG = config('INFLUX_ORG')

    parser = argparse.ArgumentParser(description="Obtain hourly consumption data \
    from the Caruna API and load it into InfluxDB. If no start/end date are \
    provided, it will obtain the previous day's data.")
    
    parser.add_argument("--startdate", type=str, required=False, 
                        help="Start date in ISO format")
    parser.add_argument("--enddate", type=str, required=False, 
                        help="End date in ISO format")
    
    args = parser.parse_args()

    if args.startdate is not None:
        try:
            sd = date.fromisoformat(args.startdate)
        except:
            print('Incorrect start date format')
            return
    else:
        sd = date.today() - timedelta(days=1)

    if args.enddate is not None:
        try:
            ed = date.fromisoformat(args.enddate)
        except:
            print('Incorrect end date format')
            return
    else:
        ed = date.today()
    
    if (ed - sd) / timedelta(days=1) > 1850:
        print("Time range too large")
        return    

    s = pycaruna.login_caruna(CARUNA_USERNAME, CARUNA_PASSWORD)
    caruna_data = pycaruna.get_cons_hours(s, CARUNA_CUSTOMER_NUM, 
        CARUNA_METERING_POINT_NUM, sd.isoformat(), ed.isoformat())

    pycaruna.logout_caruna(s)

    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, 
            org=INFLUX_ORG) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        for entry in caruna_data:
            if entry['hourlyMeasured']:
                d = {
                    "measurement": "carunaAPI",
                    "fields": {
                        "consumption": float(entry['values']['EL_ENERGY_CONSUMPTION#0']['valueAsFloat'])
                    },
                    "tags": {
                        "metering_point": CARUNA_METERING_POINT_NUM,
                        "customer_number": CARUNA_CUSTOMER_NUM,
                        "status": entry['values']['EL_ENERGY_CONSUMPTION#0']['statusAsSerieStatus'],
                    },
                    "time": datetime(int(entry['year']),
                                    int(entry['month']), 
                                    int(entry['day']),
                                    int(entry['hour']),
                                    tzinfo=timezone(timedelta(hours=int(entry['utcOffset']))))
                } 
                write_api.write(INFLUX_BUCKET, INFLUX_ORG, Point.from_dict(d))
        
        client.close()

    return


if __name__ == "__main__":
    main()
