# import json
#
# from fastapi import FastAPI
# from pydantic import BaseModel
# import google.generativeai as genai
# from fastapi.middleware.cors import CORSMiddleware
#
# import os
# import os
# from typing import List
# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel
# from dotenv import load_dotenv
# from faiss_index import FaissIndex
# from langchain_community.chat_models import ChatOpenAI
# from langchain.prompts import ChatPromptTemplate
# from langchain.schema.output_parser import StrOutputParser
# from langchain_core.runnables import RunnableParallel, RunnablePassthrough
#
# # Load environment variables
# load_dotenv(dotenv_path='.env')
# index_path = os.getenv("INDEX_PATH_PDF")
#
# GEMINI_API_KEY = 'AIzaSyDNPTNh3hw-6Y0S6XH4wRLMTNgVGMoqSlw'
# genai.configure(api_key=GEMINI_API_KEY)
# app = FastAPI()
# origins = [
#     "http://localhost:3000",  # React development server
#     "http://localhost:50733"  # Your specific port if different
# ]
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
# model = genai.GenerativeModel('gemini-1.5-flash-latest')
# chat = model.start_chat(history=[])
# class UserResponse(BaseModel):
#     user_response: str
# # Define the data model for the request
# class QuestionRequest(BaseModel):
#     question: str
#
# # Define the data model for the response
# class QuestionResponse(BaseModel):
#     questionnumber: int
#     questions: str
#     correctanswer: str
#     explanation: str
#     categories: str
#     options: dict
#
# faiss = FaissIndex(index_path)
# faiss.load_index()
# retriever = faiss.get_retriever()
# llm = ChatOpenAI(temperature=0.7, model_name="gpt-4o")
#
# # Define the prompt templates
# _STANDALONE_QUESTION_TEMPLATE = """
# ... (Your existing prompt template here) ...
# """
# STANDALONE_QUESTION_PROMPT = ChatPromptTemplate.from_template(_STANDALONE_QUESTION_TEMPLATE)
#
# # Define the chain
# def format_docs(docs):
#     return "\n\n".join(f"Question {i}\n{doc.page_content}" for i, doc in enumerate(docs))
#
# rag_chain_from_docs = (
#     RunnablePassthrough.assign(context=(lambda x: format_docs(x["context"])))
#     | STANDALONE_QUESTION_PROMPT
#     | llm
#     | StrOutputParser()
# )
#
# pdf_question_chain = RunnableParallel(
#     {"context": retriever, "question": RunnablePassthrough()}
# ).assign(answer=rag_chain_from_docs)
#
# # Define the FastAPI endpoint
# @app.post("/generate-questions", response_model=List[QuestionResponse])
# async def generate_questions(request: QuestionRequest):
#     try:
#         result = pdf_question_chain.invoke({"question": request.question})
#         return result
#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
#
# @app.post("/gemini_explanation")
# async def get_new_explanation(user_response: UserResponse):
#     # Assuming `chat.send_message()` is some function you have elsewhere
#     response = chat.send_message(f"""
#           You are an 10+ years experienced UPSC educator. You have to give explanation for
#           the mcq questions that is sent to you as input {user_response.user_response} which is a stringified json.
#           Generate a detailed explanation as string stating why the particular option is correct and why others are wrong.
#           In order to give detailed explanation to the UPSC aspirants.
#     """)
#     raw_text = response.text
#     print(raw_text)
#     raw_text = raw_text.replace("`", "")
#     raw_text = raw_text.replace("json", "")
#
#     return {"Message": "Gemini explanation", "explanation": raw_text}
#
#
# @app.get("/gemini_generatequestions")
# async def get_questions(topic: str):
#     # Assuming `chat.send_message()` is some function you have elsewhere
#     response = chat.send_message(f"""
#           You are an 10+ years experienced technical education expert. Your task is to generate 10 questions
#            based on the prompt provided by the user {topic}. Analyse the prompt correctly and generate the
#            questions which are on the level of GATE exams an entrance exam for higher studies in
#            premier institutes that are conducted in INDIA. Strictly make sure that the output is in form of json array
#            which consists on questions, option_a, option_b, option_c, option_d, correct_answer, explanation,
#            categories to which the question belongs to. And strictly ensure that i will convert ur output string to json so i shouldn't get any json parse error be careful on this""")
#     raw_text = response.text
#     raw_text = raw_text.replace("`", "")
#     raw_text = raw_text.replace("\n", "")
#     raw_text = raw_text.replace("\\", "")
#     raw_text = raw_text.replace("json", "")
#     print(raw_text)
#     questions = json.loads(raw_text)
#
#     # Ensure the questions is a list
#     if not isinstance(questions, list):
#         raise ValueError("The response is not a list")
#
#     return {"Message": "Gemini questions", "questions": questions}
#
#
#
#
# @app.get("/gemini_performanceanalysis")
# async def get_performance_analysis(topic: str):
#     # Assuming `chat.send_message()` is some function you have elsewhere
#     response = chat.send_message(f"""
#           You are an 10+ years experienced technical education expert. Your task is to generate a
#           brief summary helping the user to use this as an input for planning their further studies
#           plan to excel in the exam. Focus on informing them their weaker and stronger areas and also
#           help them to make themselves stronger. A string which contains of category and scores where
#           multiple category-score is comma separated {topic}. Give the output in form of a string and also adhere to
#           the points that are given.
#     """)
#     raw_text = response.text
#     print(raw_text)
#     raw_text = raw_text.replace("`", "")
#     raw_text = raw_text.replace("json", "")
#     raw_text = raw_text.replace("*", "")
#
#     return {"Message": "Gemini analysis", "feedback": raw_text}
#
#
# @app.get("/gemini_prediction")
# async def get_performance_analysis(pastdata: str, currentdata: str):
#     # Assuming `chat.send_message()` is some function you have elsewhere
#     response = chat.send_message(f"""
#           You are an intelligent agent good in predicting that whether if a question is skipped by the user
#           what is the probability that they might had attempted correct or wrong. Also give the
#           explanation about ur prediction why do u think so that too  based on the history and the
#           current data. Analyse the past performance data {pastdata} where the userscore on particular category will be shared with you as comma separated values
#            and look onto the present response {currentdata} where u will have the question, category and whether user has answered or
#            skipped and on the basis of this generate a short summary around 8 lines as an output by adhering to the rules i mentioned. If past data is not available then motivate the user to take the test for future predictions.
#     """)
#     raw_text = response.text
#     print(raw_text)
#     raw_text = raw_text.replace("`", "")
#     raw_text = raw_text.replace("json", "")
#     raw_text = raw_text.replace("*", "")
#
#     return {"Message": "Gemini prediction", "prediction": raw_text}
#
# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8000)


import json

import httpx
import requests
from fastapi import FastAPI
from pydantic import BaseModel
import google.generativeai as genai
from fastapi.middleware.cors import CORSMiddleware

import os
import os
from typing import List
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from dotenv import load_dotenv
from faiss_index import FaissIndex
from langchain_community.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema.output_parser import StrOutputParser
from langchain_core.runnables import RunnableParallel, RunnablePassthrough
from langserve import add_routes
from pdf_question_chain import pdf_question_chain
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv(dotenv_path='.env')
index_path = os.getenv("INDEX_PATH_PDF")

GEMINI_API_KEY = 'AIzaSyDNPTNh3hw-6Y0S6XH4wRLMTNgVGMoqSlw'
genai.configure(api_key=GEMINI_API_KEY)
app = FastAPI()
origins = [
    "http://localhost:3000",  # React development server
    "http://localhost:50733",  # Your specific port if different
    "http://localhost:8000"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
model = genai.GenerativeModel('gemini-1.5-flash-latest')
chat = model.start_chat(history=[])


class UserResponse(BaseModel):
    user_response: str


# Define the data model for the request
class QuestionRequest(BaseModel):
    question: str


# Define the data model for the response
class QuestionResponse(BaseModel):
    questionnumber: int
    questions: str
    correctanswer: str
    explanation: str
    categories: str
    options: dict


add_routes(app, pdf_question_chain, path="/generate_question")


# async def send_batch_to_api(batch_data: str):
#     api_endpoint = 'http://localhost:8000/generate_question/invoke'
#     headers = {'Content-Type': 'application/json'}
#     json_data = {
#         "input": batch_data,
#         "config": {},
#         "kwargs": {}
#     }
#     successful_responses = []
#     async with httpx.AsyncClient() as client:
#         response = await client.post(api_endpoint, headers=headers, json=json_data)
#         if response.status_code == 200:
#             response_data = response.json()
#             if response_data is not None:
#                 response_new = response_data.get('output', "{}").replace("`", "")
#                 response_new = response_new.replace("\n", "")
#                 response_new = response_new.replace("\\", "")
#                 response_new = response_new.replace("json", "")
#                 response_format = json.loads(response_new)
#                 successful_responses.extend(response_format)
#                 return successful_responses
#         else:
#             print(f"API request failed with status code: {response.status_code}")
#             raise HTTPException(status_code=500, detail=f"API request failed with status code: {response.status_code}")
async def send_batch_to_api(batch_data: str):
    api_endpoint = 'http://localhost:8000/generate_question'
    headers = {'Content-Type': 'application/json'}
    json_data = {
        "input": batch_data,
        "config": {},
        "kwargs": {}
    }
    async with httpx.AsyncClient() as client:
        try:
            logger.info(f"Sending request to {api_endpoint} with data: {json_data}")
            response = await client.post(api_endpoint, headers=headers, json=json_data)
            response.raise_for_status()
            response_data = response.json()
            logger.info(f"Response received: {response_data}")
            response_new = response_data.get('output', "{}").replace("`", "")
            response_new = response_new.replace("\n", "")
            response_new = response_new.replace("\\", "")
            response_new = response_new.replace("json", "")
            response_format = json.loads(response_new)
            return response_format
        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error occurred: {e}")
            raise HTTPException(status_code=500, detail=f"API request failed with status code: {e.response.status_code}")
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise HTTPException(status_code=500, detail=str(e))

@app.post("/gemini_explanation")
async def get_new_explanation(user_response: UserResponse):
    # Assuming `chat.send_message()` is some function you have elsewhere
    response = chat.send_message(f"""
          You are an 10+ years experienced UPSC educator. You have to give explanation for 
          the mcq questions that is sent to you as input {user_response.user_response} which is a stringified json.
          Generate a detailed explanation as string stating why the particular option is correct and why others are wrong.
          In order to give detailed explanation to the UPSC aspirants.
    """)
    raw_text = response.text
    print(raw_text)
    raw_text = raw_text.replace("`", "")
    raw_text = raw_text.replace("json", "")

    return {"Message": "Gemini explanation", "explanation": raw_text}


@app.get("/gemini_generatequestions")
async def get_questions(topic: str):
    # Assuming `chat.send_message()` is some function you have elsewhere
    response = chat.send_message(f"""
          You are an 10+ years experienced technical education expert. Your task is to generate 10 questions
           based on the prompt provided by the user {topic}. Analyse the prompt correctly and generate the 
           questions which are on the level of GATE exams an entrance exam for higher studies in 
           premier institutes that are conducted in INDIA. Strictly make sure that the output is in form of json array 
           which consists on questions, option_a, option_b, option_c, option_d, correct_answer, explanation, 
           categories to which the question belongs to. And strictly ensure that i will convert ur output string to json so i shouldn't get any json parse error be careful on this""")
    raw_text = response.text
    raw_text = raw_text.replace("`", "")
    raw_text = raw_text.replace("\n", "")
    raw_text = raw_text.replace("\\", "")
    raw_text = raw_text.replace("json", "")
    print(raw_text)
    questions = json.loads(raw_text)

    # Ensure the questions is a list
    if not isinstance(questions, list):
        raise ValueError("The response is not a list")

    return {"Message": "Gemini questions", "questions": questions}


@app.get("/gpt_generatequestions")
async def generate_questions(batch_data: str):
    return await send_batch_to_api(batch_data)


@app.get("/gemini_performanceanalysis")
async def get_performance_analysis(topic: str):
    # Assuming `chat.send_message()` is some function you have elsewhere
    response = chat.send_message(f"""
          You are an 10+ years experienced technical education expert. Your task is to generate a 
          brief summary helping the user to use this as an input for planning their further studies 
          plan to excel in the exam. Focus on informing them their weaker and stronger areas and also 
          help them to make themselves stronger. A string which contains of category and scores where 
          multiple category-score is comma separated {topic}. Give the output in form of a string and also adhere to 
          the points that are given.
    """)
    raw_text = response.text
    print(raw_text)
    raw_text = raw_text.replace("`", "")
    raw_text = raw_text.replace("json", "")
    raw_text = raw_text.replace("*", "")

    return {"Message": "Gemini analysis", "feedback": raw_text}


@app.get("/gemini_prediction")
async def get_performance_analysis(pastdata: str, currentdata: str):
    # Assuming `chat.send_message()` is some function you have elsewhere
    response = chat.send_message(f"""
          You are an intelligent agent good in predicting that whether if a question is skipped by the user 
          what is the probability that they might had attempted correct or wrong. Also give the 
          explanation about ur prediction why do u think so that too  based on the history and the 
          current data. Analyse the past performance data {pastdata} where the userscore on particular category will be shared with you as comma separated values
           and look onto the present response {currentdata} where u will have the question, category and whether user has answered or 
           skipped and on the basis of this generate a short summary around 8 lines as an output by adhering to the rules i mentioned. If past data is not available then motivate the user to take the test for future predictions.
    """)
    raw_text = response.text
    print(raw_text)
    raw_text = raw_text.replace("`", "")
    raw_text = raw_text.replace("json", "")
    raw_text = raw_text.replace("*", "")

    return {"Message": "Gemini prediction", "prediction": raw_text}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
