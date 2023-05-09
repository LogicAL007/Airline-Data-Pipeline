from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import jsonschema
from kafka import KafkaProducer
import json
app = FastAPI()

# Define Pydantic model with schema
class Flight(BaseModel):
    SerialNo: int
    airline: str
    flight: str
    source_city: str
    departure_time: str
    stops: str
    arrival_time: str
    destination_city: str
    class_: str = None
    duration: float
    days_left: int
    price: float

# Base URL
@app.get("/")
async def root():
    return {"message": "Hello World"}

schema = {
    "type": "object",
    "properties": {
        "SerialNo": {"type": "integer"},
        "airline": {"type": "string"},
        "flight": {"type": "string"},
        "source_city": {"type": "string"},
        "departure_time": {"type": "string"},
        "stops": {"type": "string"},
        "arrival_time": {"type": "string"},
        "destination_city": {"type": "string"},
        "class": {"type": "string"},
        "duration": {"type": "number"},
        "days_left": {"type": "integer"},
        "price": {"type": "number"}
    },
    "required": ["SerialNo", "airline", "flight", "source_city", "departure_time", "stops", "arrival_time", "destination_city", "duration", "days_left", "price"]
}

# Load the JSON data from a file or variable
@app.post("/flight")
async def post_flight_information(item: Flight):
    print("message collected")
    try:
        # Convert Pydantic model to dict
        item_dict = item.dict()
        
        # Validate against schema
        jsonschema.validate(item_dict, schema)
    
        # Produce the string
        produce_kafka_string(item_dict)
        # Encode the created customer item if successful into a JSON and return it to the client with 201
        return JSONResponse(content=item_dict, status_code=201)
    
    except ValueError:
        return JSONResponse(content=jsonable_encoder(item), status_code=400)

def produce_kafka_string(item_dict):
    # Create producer
        producer = KafkaProducer(bootstrap_servers='localhost:9093',acks=1)
        
        # Write the string as bytes because Kafka needs it this way
        producer.send('my-topic', bytes(item_dict, 'utf-8'))
        producer.flush()
    
def success(metadata):
    print(metadata.topic)

def error(exception):
    print(exception)  