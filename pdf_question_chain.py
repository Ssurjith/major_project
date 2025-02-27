
import os

from operator import itemgetter
from typing import List

from langchain_community.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.schema import format_document
from langchain.schema.output_parser import StrOutputParser
from langchain_core.runnables import RunnableParallel, RunnablePassthrough, RunnableLambda
from faiss_index import FaissIndex

from dotenv import load_dotenv
from typing import Any, Callable, Dict, List, Optional, TypedDict

load_dotenv(dotenv_path='.env')

index_path = os.getenv("INDEX_PATH_PDF")
print(index_path)

# Standalone question template, removed chat history
_STANDALONE_QUESTION_TEMPLATE = """
You are an AI assistant tasked with generating practice questions for competitive exams in India, based on a provided context of recent developments. Analyze the data in the context, which includes
recent developments and updates. Your goal is to produce new questions that could potentially appear in future exams,
closely mirroring the style, structure, and content of the provided examples. Adhere to the following guidelines:
Use the following pieces of context to generate questions.
1.
Context Analysis: Analyse and understand the data provided in the context and extract the key concepts and be relvant to the updated information.Use the data only from the provided context and generate the questions

2. Question Structure:

Clarity and Conciseness: Questions are worded clearly and concisely, avoiding ambiguity or unnecessary complexity.
Variety in Format: UPSC exams utilize various question formats like:
Statement-based questions: Analyze a statement and ask related questions (e.g., reasons behind it, its implications).
Number of statements correct: Assess understanding by asking how many statements from a set are true (often with a mix of true/false statements).
Direct questions: These test knowledge application and higher-order thinking skills. They might ask for analysis, evaluation, suggesting solutions, or comparing viewpoints on a topic.
Case studies: Analyze a real-world scenario and ask questions about its causes, effects, or potential solutions (often in Essay papers).
Difficulty Level:

Graded Difficulty: Papers typically have a mix of easy, moderate, and difficult questions to assess a wide range of knowledge levels.
Focus on Critical Thinking: The aim is to go beyond simple recall of facts. Questions often require candidates to analyze data, interpret information, form well-reasoned arguments, and demonstrate critical thinking skills.
Ethical Dilemmas: Some questions might present ethical dilemmas related to governance, policy, or social issues. Candidates are expected to analyze the situation and suggest well-considered responses.
Additional Tips:

UPSC Trends: Stay updated on current affairs and emerging issues relevant to the UPSC syllabus. Questions often reflect these trends.
Language and Presentation: Questions are presented in grammatically correct and formal English.

3. Detailed Responses: For each question, provide a correct answer and a detailed explanation of why that answer is
correct. Include categorization details such as the relevant sectors, topics, and main keywords.

4. Format Requirements: Return the questions in a JSON array, each represented as an object containing the following
fields:
    - questionnumber: Starting from 1 for the first question.
    - questions: The text of the question.
    - correctanswer: The index character of the correct option ('a', 'b', 'c','d').
    - explanation: Explanation for why the answer is correct.
    - categories: A string listing relevant sectors, topics, and keywords.
    - options: An object with keys 'a', 'b', 'c', 'd' representing possible answers.

5. Adherence to Request: Generate exactly the number of questions requested—no more, no less. Ensure that all 
generated questions maintain the strict format of multiple-choice as demonstrated in the context. Also be sure that u 
don't miss the statements in multiple statementwise questions .

Context:
{context}

User:
{question}

Suggested Questions:
  """

STANDALONE_QUESTION_PROMPT = ChatPromptTemplate.from_template(_STANDALONE_QUESTION_TEMPLATE)

DEFAULT_DOCUMENT_PROMPT = ChatPromptTemplate.from_template(template="{page_content}")


def format_docs(docs):
    formatted_docs = "\n\n".join(f"Question {i}\n{doc.page_content}" for i, doc in enumerate(docs))
    print(index_path)
    print(formatted_docs)
    return formatted_docs

#0 to 1 it gives efficient knld on qs
llm = ChatOpenAI(temperature=0.7, model_name="gpt-3.5-turbo-0125")
# llm = ChatOpenAI(temperature=0.7, model_name="gpt-4o")

# llm = ChatOpenAI()
faiss = FaissIndex(index_path)
faiss.load_index()
retriever = faiss.get_retriever()

rag_chain_from_docs = (
        RunnablePassthrough.assign(context=(lambda x: format_docs(x["context"])))
        | STANDALONE_QUESTION_PROMPT
        | llm
        | StrOutputParser()
)

pdf_question_chain = RunnableParallel(
    {"context": retriever, "question": RunnablePassthrough()}
).assign(answer=rag_chain_from_docs)





