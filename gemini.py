from langchain.prompts import PromptTemplate
from langchain_google_vertexai import ChatVertexAI
from langchain_core.output_parsers import StrOutputParser

def generate_answer(question, contexts):

  prompt_template = """
  Retrieved contexts:
  {context}

  Instruction:
  You must use the provided contexts to answer the following question accurately and completely. 
  Always respond in the same language as the question, whether it is in English, Spanish, or any other language.
  If the answer is not in the provided contexts, clearly state that.

  Question:
  {question}

  Answer (in the same language as the question):
  """

  prompt = PromptTemplate(
    input_variables=["context", "question"],
    template=prompt_template
  )

  llm = ChatVertexAI(
    model="gemini-1.5-flash-001",
    temperature=0
  )

  #Build context form list
  all_context = "\n".join([f'"{context}"' for context in contexts])

  chain = prompt | llm | StrOutputParser()
  res = chain.invoke(input={"context": all_context, "question": question})

  return res