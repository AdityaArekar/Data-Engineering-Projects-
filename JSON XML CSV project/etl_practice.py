from datetime import datetime
import glob
import pandas as pd 
import xml.etree.ElementTree as ET

log_file = "log_file.txt"
target_file = "transformed_data.csv"

def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)

    return dataframe


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)

    return dataframe


def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["])