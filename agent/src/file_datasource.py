import csv
from datetime import datetime
from typing import List

import config
from domain.accelerometer import Accelerometer
from domain.gps import Gps
from domain.parking import Parking
from domain.aggregated_data import AggregatedData


class FileDatasource:
    def __init__(
            self,
            accelerometer_filename: str,
            gps_filename: str,
            parking_filename: str,
    ) -> None:
        self.accelerometer_filename = accelerometer_filename
        self.gps_filename = gps_filename
        self.parking_filename = parking_filename
        self.open_accelerometer_file = None
        self.open_gps_file = None
        self.open_parking_file = None

    def read(self) -> List[AggregatedData]:
        """Метод повертає дані отримані з датчиків"""

        accelerometer_data = self.read_accelerometer_data()
        gps_data = self.read_gps_data()
        parking_data = self.read_parking_data()

        aggregated_data = []
        for accelerometer, gps, parking in zip(accelerometer_data, gps_data, parking_data):
            aggregated_data.append(
                AggregatedData(
                    accelerometer,
                    gps,
                    parking,
                    datetime.now(),
                    config.USER_ID,
                )
            )

        return aggregated_data

    def startReading(self):
        """Метод повинен викликатись перед початком читання даних"""
        try:
            self.open_accelerometer_file = open(self.accelerometer_filename, "r")
            self.open_gps_file = open(self.gps_filename, "r")
            self.open_parking_file = open(self.parking_filename, "r")

            next(self.open_accelerometer_file)
            next(self.open_gps_file)
            next(self.open_parking_file)
        except Exception as e:
            print(f"An error occurred: {e}")

    def stopReading(self, *args, **kwargs):
        """Метод повинен викликатись для закінчення читання даних"""
        try:
            if self.open_accelerometer_file:
                self.open_accelerometer_file.close()
            if self.open_gps_file:
                self.open_gps_file.close()
            if self.open_parking_file:
                self.open_parking_file.close()

        except Exception as e:
            print(f"An error occurred while closing files: {e}")

    def read_accelerometer_data(self) -> List[Accelerometer]:
        accelerometer_data = []
        with open(self.accelerometer_filename, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # skip header
            for row in reader:
                x, y, z = map(int, row)
                accelerometer_data.append(Accelerometer(x, y, z))
        return accelerometer_data

    def read_gps_data(self) -> List[Gps]:
        gps_data = []
        with open(self.gps_filename, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # skip header
            for row in reader:
                longitude, latitude = map(float, row)
                gps_data.append(Gps(longitude, latitude))
        return gps_data

    def read_parking_data(self) -> List[Parking]:
        parking_data = []
        with open(self.parking_filename, 'r') as file:
            reader = csv.reader(file)
            next(reader)  # skip header
            for row in reader:
                empty_count, longitude, latitude = map(float, row)
                gps = Gps(longitude, latitude)
                parking_data.append(Parking(int(empty_count), gps))
        return parking_data
