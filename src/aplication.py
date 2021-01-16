from typing import Optional
from fastapi import FastAPI
from pydantic import BaseModel
from prometheus_client import Summary, Counter
from starlette_prometheus import metrics, PrometheusMiddleware

app = FastAPI()
app.add_middleware(PrometheusMiddleware)
app.add_route("/metrics/", metrics)

# Create a metric to track time spent and requests made.
REQUEST_TIME = Summary('EU_request_processing_seconds', 'Time spent processing request')
c = Counter('my_failures', 'Description of counter')

class Item(BaseModel):
    name: str
    price: float
    is_offer: Optional[bool] = None

@app.get("/")
def read_root():
    return {"Hello World"}


# Decorate function with metric.
@REQUEST_TIME.time()
@app.get("/metricas")
def get_metrics():
    """
    docstring
    """
    c.inc()
    return "METRICA 1"

@app.get("/items/{item_id}")
def read_item(item_id: int, q: Optional[str] = None):
    c.inc()     # Increment by 1
    return {"item_id": item_id, "q": q}

@app.put("/items/{item_id}")
def update_item(item_id: int, item: Item):
    return {"item_name": item.name, "item_id": item_id}

# executar com "uvicorn aplication:app --reload"