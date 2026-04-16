# main.py
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from passlib.context import CryptContext
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker, declarative_base, Session
import jwt
import datetime
from fastapi import APIRouter

# ------------------ Configuration ------------------
DATABASE_URL = "sqlite:///./users.db"
SECRET_KEY = "YOUR_SECRET_KEY_HERE"  # Replace with a secure key
ALGORITHM = "HS256"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)
Base = declarative_base()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
router = APIRouter()

# ------------------ Database Models ------------------
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    password = Column(String)

# Create tables
Base.metadata.create_all(bind=engine)

# ------------------ Pydantic Schemas ------------------
class UserRegister(BaseModel):
    username: str
    password: str

class UserLogin(BaseModel):
    username: str
    password: str

# ------------------ Utility Functions ------------------   
def get_db():
    db = SessionLocal()
    try:
        yield db          
    finally:
        db.close()

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)

def create_token(username: str) -> str:
    payload = {
        "sub": username,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(hours=24)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

# ------------------ Routes ------------------
@router.post("/register")


def register(user: UserRegister, db: Session = Depends(get_db)):
    existing_user = db.query(User).filter(User.username == user.username).first()
    if existing_user:
        raise HTTPException(status_code=400, detail="User Already Exist! Try To Login With Same User name and password")
    
    hashed = hash_password(user.password)
    new_user = User(username=user.username, password=hashed)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    
    
    return {"message": "User created Successfully"}
@router.post("/login")
def login(user: UserLogin, db: Session = Depends(get_db)):
    db_user = db.query(User).filter(User.username == user.username).first()
    if not db_user or not verify_password(user.password, db_user.password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    token = create_token(user.username)
    return {"access_token": token, "token_type": "bearer"}