import argparse, json, pycaruna
from datetime import date, datetime, timezone, timedelta
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from decouple import config

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

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

    (session, info) = pycaruna.login_caruna(CARUNA_USERNAME, CARUNA_PASSWORD)
    token = info['token']

    caruna_data = []
    for day in daterange(sd, ed + timedelta(days=1)):
        caruna_data.append(pycaruna.get_cons_hours(session, token, 
            CARUNA_CUSTOMER_NUM, CARUNA_METERING_POINT_NUM, str(day.year),
            str(day.month), str(day.day)))

    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, 
            org=INFLUX_ORG) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)

        for entry in caruna_data:
            for hour in entry['results'][0]['data']:
                if hour['consumption']:
                    d = {
                        "measurement": "hourlyConsumption",
                        "fields": {
                            "value": float(hour['consumption'])
                        },
                        "time": datetime.fromisoformat(hour['timestamp'][:-1])
                    } 
                    write_api.write(INFLUX_BUCKET, INFLUX_ORG, Point.from_dict(d))
        
        client.close()

    return


if __name__ == "__main__":
    main()
