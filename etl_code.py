import glob
import pandas as pd  
import xml.etree.ElementTree as ET
from datetime import datetime

#Creating a file where log data will be stored
log_file = "log_file.txt"

#Creating a file where final output data will be stored
target_file = "transformed_data.csv"

#STEP 1 ---------- EXTRACTION ------------

#Declaring 3 functions to extract data from CSV, JSON, XML file. Also need to pass "file_to_process" argument.

def extract_from_JSON(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe
    
def extract_from_CSV(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe

def extract_from_XML(file_to_process):
    dataframe = pd.DataFrame(columns=["name","height","weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])],ignore_index = True)
    return dataframe
    
#Create a function to identify the file type and call the above functions accordingly

def extract():

    #create a empty space to store the extracted data
    extracted_data = pd.DataFrame(columns = ["name", "height", "weight"])

    for csvfile in glob.glob("*csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_CSV(csvfile))], ignore_index=True)

    for jsonfile in glob.glob("*json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_JSON(jsonfile))],ignore_index=True)

    for xmlfile in glob.glob("*xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_XML(xmlfile))],ignore_index=True)

    return extracted_data


#STEP 2 -------- TRANSFORMATION ---------

#transaformation is necessary because height is in inches and weight is in pounds.
#for our application we need height in meters and weight in kilograms
#so we will need a function to perform unit conversion for the two parameters

def transform(data):
    '''Convert inches to meters and round off to two decimals
        1 inch is 0.0254 meters '''
    
    data['height'] = round(data.height * 0.0254,2)

    '''Convert pounds to kilograms and round off to two decimals 
        1 pound is 0.45359237 kilograms '''
    
    data['weight'] = round(data.weight * 0.45359237,2)

    return data


# ---------- LOADING DATA -----------

def load_data(target_file, transformed_data):
    transformed_data.to_csv(target_file)


def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ',' + message + '\n')


# ------- LOGGING --------

# log the initialization of the ETL process
log_progress("ETL job started")


# log the beginning of the extraction process
log_progress("Extract phase started")
extracted_data = extract()

# log the completion of the extraction process
log_progress("Extract phase ended")

# log the beginning of the Transformation process
log_progress("Transform phase started")
transformed_data = transform(extracted_data)
print("Transformed Data")
print(transformed_data)

# log the completion of the transformation process
log_progress("Transform phase ended")

# log the beginning of the loading process
log_progress("Load Phase Started")
load_data(target_file,transformed_data)

# log the completion of the ETL process
log_progress("ETL job ended")

