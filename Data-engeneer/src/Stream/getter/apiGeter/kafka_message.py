from fastapi import APIRouter,UploadFile,File
from middle.kafka_transformation.consumer_connection import Consumer
from middle.kafka_transformation.producer_matomo import Product
import json
router=APIRouter()

@router.post("/send_message")
async def send_message(topic:str,file: UploadFile = File(...) ):
    """
    api post message from product to broker
    input: 
        data: data tracking from source (Each field is marked with a " ")
        topic: source name
    """
    content=await file.read()
    json_data=content.decode("utf-8")
    # if data is None: return {"status": "Data none"}
    # json_data=json.dumps(data)
    product=Product(topic)
    product.send_message(json_data)
    return {"status": "200","file_name":file.filename}
@router.get("/get_message")
def get_message(topic: str):
    """
    api get message from broker
    input: topic
    output : message from kafka of this topic
    """
    print(f"topic: {topic}")
    consumer=Consumer(topic)
    print("create consumer")
    filename=consumer.get_message()
    return {"status":"ok", "filename": filename}    
