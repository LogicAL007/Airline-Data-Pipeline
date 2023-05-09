from fastapi import FastAPI
# You need this to be able to turn classes into JSONs and return
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
# Needed for json.dumps
import json

# Both used for BaseModel
from pydantic import BaseModel

from datetime import datetime
from kafka import KafkaProducer, producer

class Flight(BaseModel):
    SerialNo:int
    airline:str 
    flight:str
    source_city:str
    departure_time:str
    stops:str 
    arrival_time:str
    destination_city:str
    duration:float
    days_left:int
    price:int 

app = FastAPI()

# Base URL
@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.post("/flight")
async def post_flight_information(item: Flight):
    print("message collected")
    try:
        json_of_item = jsonable_encoder(item)
        
        # Dump the json out as string
        json_as_string = json.dumps(json_of_item)
        print(json_as_string)
        
        # Produce the string
        #produce_kafka_string(json_as_string)

        # Encode the created customer item if successful into a JSON and return it to the client with 201
        return JSONResponse(content=json_of_item, status_code=201)
    
    except ValueError:
        return  JSONResponse(content=jsonable_encoder(item), status_code=400)
    
def produce_kafka_string(json_as_string):
    # Create producer
        producer = KafkaProducer(bootstrap_servers='kafka:9092',acks=1)
        
        # Write the string as bytes because Kafka needs it this way
        producer.send('ingestion-topic', bytes(json_as_string, 'utf-8'))
        producer.flush()