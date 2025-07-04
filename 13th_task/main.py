from fastapi import FastAPI, HTTPException, Path
from .connect import get_db_connection,read_config
from pydantic import BaseModel
import pyodbc

app = FastAPI()

# Pydantic model
class customer(BaseModel):
    customer_id: int
    name: str
    email:str
    phone: str
    address: str



@app.get("/")
def root():
    return {"message": "Welcome to the Customer API"}


# get all customers
@app.get("/customer")
def real_all_customer():
    config = read_config()
    conn = get_db_connection(config)
    if not conn:
        raise HTTPException(status_code = 500,detail="Datbase connection failed")
    cursor = conn.cursor()
    cursor.execute("SELECT customer_id,name,email,phone,address FROM customer")
    rows = cursor.fetchall()
    conn.close()
    return [dict(zip([column[0] for column in cursor.description],row)) for row in rows]

# get customers by ID
@app.get("/customer/{id}")
def read_customer(id: int = Path(..., description="The ID of the customer to retrieve")):
    config = read_config()
    conn = get_db_connection(config)
    if not conn:
        raise HTTPException(status_code = 500,detail="Datbase connection failed")
    cursor = conn.cursor()
    cursor.execute("SELECT customer_id,name,email,phone,address FROM customer WHERE customer_id = ?",id)
    row = cursor.fetchone()
    conn.close()
    if row:
        return dict(zip([column[0] for column in cursor.description],row))
    else:
        raise HTTPException(status_code=404,detail="Customer not found")

#create a new customer
@app.post("/customer/")
def create_customer(customer: customer):
    config = read_config()
    conn = get_db_connection(config)
    if not conn:
        raise HTTPException(status_code = 500,detail="Datbase connection failed")
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO customer(customer_id,name,email,phone,address) VALUES(?,?,?,?,?)",
          (customer.customer_id,customer.name,customer.email,customer.phone,customer.address)
    )
    conn.commit()
    conn.close()
    return {"message" : "User created successfully"}

#update an existing customer based on customer_id
@app.put("/customer/{id}")
def update_customer(id:int,customer : customer):
    config = read_config()
    conn = get_db_connection(config)
    if not conn:
        raise HTTPException(status_code = 500,detail="Datbase connection failed")
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE customer SET name = ?,email = ?,phone = ?,address = ? WHERE customer_id = ?",
        (customer.name,customer.email,customer.phone,customer.address,id)
    )
    conn.commit()
    conn.close()
    return {"message": "Customer updated successfully"}

#delete an existing customer based on customer id
@app.delete("/customer/{id}")
def delete_customer(id:int):
    config = read_config()
    conn = get_db_connection(config)
    if not conn:
        raise HTTPException(status_code = 500,detail="Datbase connection failed")
    cursor = conn.cursor()
    cursor.execute("DELETE FROM customer WHERE customer_id = ?",id)
    conn.commit()
    conn.close()
    return {"message" : "Customer deleted successfully"}
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,host = "127.0.0.1",port=8001)